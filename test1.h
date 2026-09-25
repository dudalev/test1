#pragma once

// Derived-value publisher: a fast market-data feed in, a slower consumer out.

#include <algorithm>
#include <array>
#include <cctype>
#include <charconv>
#include <chrono>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <expected>
#include <format>
#include <functional>
#include <iterator>
#include <map>
#include <optional>
#include <ostream>
#include <print>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace test1 {

using Millis    = std::chrono::milliseconds;
using Timestamp = std::chrono::sys_time<Millis>;

constexpr Timestamp    to_timestamp(std::int64_t ms) { return Timestamp{Millis{ms}}; }
constexpr std::int64_t to_ms(Timestamp t) { return t.time_since_epoch().count(); }

// ---- configuration ----

struct DefaultLimits {
    static constexpr Millis max_ahead         = Millis{5000};
    static constexpr Millis max_behind        = Millis{1000};
    static constexpr Millis max_component_age = Millis{60000};
};

// Hard bounds on a timestamp field, independent of any history: they are the only defence for
// the very first input of a feed, and they catch seconds-instead-of-milliseconds.
constexpr Timestamp   kEarliestTimestamp   = std::chrono::sys_days{std::chrono::year{2000} / 1 / 1};
constexpr Timestamp   kLatestTimestamp     = std::chrono::sys_days{std::chrono::year{2100} / 1 / 1};
constexpr std::size_t kMaxInstrumentLength = 32;

constexpr std::size_t  kDefaultQueueCapacity = 32;
constexpr std::int64_t kDefaultStalenessMs   = 5000;
constexpr std::size_t  kDefaultHardCap       = 10'000;

struct Options {
    Millis      consumer_period = Millis{100};
    std::size_t consumer_batch  = 4;
    std::size_t max_instruments = 10'000;
};

// ---- parsing ----

enum class InputType { base_rate, spread, adjustment };

constexpr std::array<std::string_view, 3> kInputTypeNames{"base_rate", "spread", "adjustment"};

constexpr std::string_view name(InputType t) { return kInputTypeNames[std::to_underlying(t)]; }

constexpr std::optional<InputType> parse_type(std::string_view text) {
    for (std::size_t i = 0; i < kInputTypeNames.size(); ++i) {
        if (kInputTypeNames[i] == text) {
            return static_cast<InputType>(i);
        }
    }
    return std::nullopt;
}

struct Input {
    Timestamp   timestamp;
    std::string instrument;
    InputType   type;
    double      value;
};

enum class Reject {
    malformed,            // wrong field count or unusable instrument name
    bad_timestamp,        // not an integer, or outside [kEarliestTimestamp, kLatestTimestamp)
    bad_type,             // input_type is not base_rate / spread / adjustment
    bad_value,            // value missing, non-numeric, or not finite (NaN, inf)
    future,               // too far ahead of the last timestamp seen for the instrument
    stale,                // too far behind the last timestamp seen for the instrument
    superseded,           // plausible, but older than the value already held for that component
    duplicate,            // same timestamp and value as the one already held for that component
    too_many_instruments, // a new instrument beyond Options::max_instruments
};

constexpr std::string_view name(Reject r) {
    constexpr std::array names{
        std::string_view{"malformed"}, std::string_view{"bad_timestamp"},
        std::string_view{"bad_type"}, std::string_view{"bad_value"},
        std::string_view{"future"}, std::string_view{"stale"},
        std::string_view{"superseded"}, std::string_view{"duplicate"},
        std::string_view{"too_many_instruments"},
    };
    static_assert(names.size() == 9);
    return names[std::to_underlying(r)];
}

// Carries whatever was parsed before the failure, so the rejection can still be charged to
// the right instrument.
struct ParseError {
    Reject                   reason;
    std::string              instrument;
    std::optional<Timestamp> timestamp;
};

constexpr std::string_view trim(std::string_view s) {
    constexpr std::string_view blank = " \t\r\n\v\f";
    while (!s.empty() && blank.contains(s.front())) {
        s.remove_prefix(1);
    }
    while (!s.empty() && blank.contains(s.back())) {
        s.remove_suffix(1);
    }
    return s;
}

template <class Number>
std::optional<Number> parse_number(std::string_view text) {
    // from_chars does not accept a leading '+'; strip one, but leave "+-" to fail.
    if (text.size() > 1 && text[0] == '+' && text[1] != '-') {
        text.remove_prefix(1);
    }
    Number value{};
    const char* const last = text.data() + text.size();
    const auto [end, ec] = std::from_chars(text.data(), last, value);
    if (ec != std::errc{} || end != last) {
        return std::nullopt;
    }
    return value;
}

inline bool valid_instrument(std::string_view s) {
    return !s.empty() && s.size() <= kMaxInstrumentLength &&
           std::ranges::all_of(s, [](unsigned char c) { return std::isgraph(c) != 0; });
}

inline std::expected<Input, ParseError> parse_line(std::string_view line) {
    const auto fields = line | std::views::split(',') |
                        std::views::transform([](auto part) { return trim(std::string_view{part}); }) |
                        std::ranges::to<std::vector>();
    if (fields.size() != 4 || !valid_instrument(fields[1])) {
        return std::unexpected(ParseError{.reason = Reject::malformed});
    }

    std::string instrument{fields[1]};
    std::optional<Timestamp> timestamp;
    const auto fail = [&](Reject reason) {
        return std::unexpected(ParseError{reason, instrument, timestamp});
    };

    const auto ms = parse_number<std::int64_t>(fields[0]);
    if (!ms || to_timestamp(*ms) < kEarliestTimestamp || to_timestamp(*ms) >= kLatestTimestamp) {
        return fail(Reject::bad_timestamp);
    }
    timestamp = to_timestamp(*ms);

    const auto type = parse_type(fields[2]);
    if (!type) {
        return fail(Reject::bad_type);
    }

    const auto value = parse_number<double>(fields[3]);
    if (!value || !std::isfinite(*value)) {
        return fail(Reject::bad_value);
    }

    return Input{*timestamp, std::move(instrument), *type, *value};
}

// ---- state ----

struct Component {
    double    value;
    Timestamp timestamp;
};

struct InstrumentState {
    std::optional<Component> base_rate;
    std::optional<Component> spread;
    std::optional<Component> adjustment;

    std::optional<Component>& get(InputType t) {
        switch (t) {
            case InputType::base_rate:  return base_rate;
            case InputType::spread:     return spread;
            case InputType::adjustment: return adjustment;
        }
        std::unreachable();
    }

    const std::optional<Component>& get(InputType t) const {
        switch (t) {
            case InputType::base_rate:  return base_rate;
            case InputType::spread:     return spread;
            case InputType::adjustment: return adjustment;
        }
        std::unreachable();
    }

    bool complete() const { return base_rate && spread && adjustment; }

    bool ready(Timestamp as_of, Millis max_age) const {
        const auto fresh = [&](const std::optional<Component>& c) {
            return c && as_of - c->timestamp <= max_age;
        };
        return fresh(base_rate) && fresh(spread) && fresh(adjustment);
    }

    double derived_value() const {
        return base_rate->value + spread->value + adjustment->value;
    }

    Timestamp latest_timestamp() const {
        return std::max({base_rate->timestamp, spread->timestamp, adjustment->timestamp});
    }

    std::string missing() const {
        std::string out;
        if (!base_rate) {
            out += "base_rate";
        }
        if (!spread) {
            if (!out.empty()) {
                out += '+';
            }
            out += "spread";
        }
        if (!adjustment) {
            if (!out.empty()) {
                out += '+';
            }
            out += "adjustment";
        }
        return out;
    }
};

struct DiscardStats {
    std::uint64_t            count = 0;
    std::optional<Timestamp> last;
};

struct Counters {
    std::uint64_t received  = 0;
    std::uint64_t accepted  = 0;
    std::uint64_t withheld  = 0;
    std::uint64_t queued    = 0;
    std::uint64_t dropped   = 0;
    std::uint64_t delivered = 0;
};

using UpdateKey = std::pair<Timestamp, std::uint64_t>;

struct Instrument {
    InstrumentState state;

    std::optional<Timestamp> last_seen;
    DiscardStats             discards;
    Counters                 counters;

    std::optional<Component>& component(InputType t) { return state.get(t); }
    const std::optional<Component>& component(InputType t) const { return state.get(t); }
    bool complete() const { return state.complete(); }
    std::string missing() const { return state.missing(); }
};

struct Update {
    Timestamp       timestamp;
    std::uint64_t   seq;
    std::string     instrument;
    InstrumentState state;
    DiscardStats    discards;

    double derived_value() const { return state.derived_value(); }
    UpdateKey key() const { return {timestamp, seq}; }
};

// ---- output ----

// Bounded by count. Min-heap delivers oldest first. When full, the oldest entry is dropped
// to keep the most recent Capacity updates.
template <std::size_t Capacity = kDefaultQueueCapacity>
class SizeBoundedQueue {
public:
    std::vector<Update> push(Update update) {
        heap_.push_back(std::move(update));
        std::push_heap(heap_.begin(), heap_.end(), newer_);
        std::vector<Update> evicted;
        while (heap_.size() > Capacity) {
            std::pop_heap(heap_.begin(), heap_.end(), newer_);
            evicted.push_back(std::move(heap_.back()));
            heap_.pop_back();
        }
        return evicted;
    }

    std::optional<Update> pop() {
        if (heap_.empty()) {
            return std::nullopt;
        }
        std::pop_heap(heap_.begin(), heap_.end(), newer_);
        auto u = std::move(heap_.back());
        heap_.pop_back();
        return u;
    }

    bool empty() const { return heap_.empty(); }
    std::size_t size() const { return heap_.size(); }
    static std::string label() { return std::format("size-bounded({})", Capacity); }

private:
    // push_heap/pop_heap are max-heaps; "newer" comparator inverts the order → oldest on top.
    struct Newer {
        bool operator()(const Update& a, const Update& b) const { return a.key() > b.key(); }
    };
    static constexpr Newer newer_{};

    std::vector<Update> heap_;
};

// Bounded by staleness. Entries older than MaxStalenessMs relative to the freshest are dropped.
// A hard size cap prevents unbounded growth. Delivers oldest first.
template <std::int64_t MaxStalenessMs = kDefaultStalenessMs, std::size_t HardCap = kDefaultHardCap>
class TimeBoundedQueue {
public:
    std::vector<Update> push(Update update) {
        auto key = update.key();
        items_.insert_or_assign(key, std::move(update));
        return evict();
    }

    std::optional<Update> pop() {
        if (items_.empty()) {
            return std::nullopt;
        }
        auto node = items_.extract(items_.begin());
        return std::move(node.mapped());
    }

    bool empty() const { return items_.empty(); }
    std::size_t size() const { return items_.size(); }
    static std::string label() { return std::format("time-bounded({}ms, cap={})", MaxStalenessMs, HardCap); }

private:
    std::vector<Update> evict() {
        std::vector<Update> evicted;
        if (items_.size() > 1) {
            const auto newest_ts = std::prev(items_.end())->first.first;
            const auto cutoff = newest_ts - Millis{MaxStalenessMs};
            while (!items_.empty() && items_.begin()->first.first < cutoff) {
                auto node = items_.extract(items_.begin());
                evicted.push_back(std::move(node.mapped()));
            }
        }
        while (items_.size() > HardCap) {
            auto node = items_.extract(items_.begin());
            evicted.push_back(std::move(node.mapped()));
        }
        return evicted;
    }

    std::map<UpdateKey, Update> items_;
};

// Just the freshest. Holds a single update; each push replaces it only if newer.
class FreshestQueue {
public:
    std::vector<Update> push(Update update) {
        std::vector<Update> evicted;
        if (item_) {
            if (update.key() < item_->key()) {
                evicted.push_back(std::move(update));
                return evicted;
            }
            evicted.push_back(std::move(*item_));
        }
        item_ = std::move(update);
        return evicted;
    }

    std::optional<Update> pop() {
        if (!item_) {
            return std::nullopt;
        }
        auto u = std::move(*item_);
        item_.reset();
        return u;
    }

    bool empty() const { return !item_.has_value(); }
    std::size_t size() const { return item_ ? 1 : 0; }
    static std::string label() { return "freshest-only(1)"; }

private:
    std::optional<Update> item_;
};

// ---- pipeline ----

template <class Limits = DefaultLimits, class Queue = SizeBoundedQueue<>>
class Publisher {
public:
    using Sink = std::function<void(const Update&, Timestamp delivered_at)>;

    explicit Publisher(Sink sink, Options options = {})
        : sink_(std::move(sink)), options_(options) {
        options_.consumer_period = std::max(options_.consumer_period, Millis{1});
    }

    std::optional<Reject> on_line(std::string_view line, std::uint64_t seq) {
        ++lines_;
        const auto parsed = parse_line(line);
        if (parsed) {
            return on_input(*parsed, seq);
        }
        const auto& [reason, instrument, timestamp] = parsed.error();
        // find but don't create — rejected lines must not consume instrument slots
        if (!instrument.empty()) {
            if (auto it = instruments_.find(instrument); it != instruments_.end()) {
                ++it->second.counters.received;
                discard(it->second, timestamp, reason);
            } else {
                ++unattributed_;
            }
        } else {
            ++unattributed_;
        }
        return reason;
    }

    void finish() {
        if (feed_clock_) {
            deliver(queue_.size(), *feed_clock_);
        }
    }

    void print_summary(std::ostream& out) const {
        constexpr auto row = "{:<34}{:>9}{:>9}{:>9}{:>9}{:>8}{:>8}{:>10}  {}";
        std::println(out, "summary: {} line(s) processed, {} rejected without a usable instrument",
                     lines_, unattributed_);
        std::println(out, row, "instrument", "received", "rejected", "accepted", "withheld", "queued",
                     "dropped", "delivered", "notes");
        for (const auto& [name, inst] : instruments_) {
            const auto& c = inst.counters;
            std::println(out, row, name, c.received, inst.discards.count, c.accepted, c.withheld,
                         c.queued, c.dropped, c.delivered, notes(inst));
        }
        std::println(out, "");
        std::println(out, "  received  = inputs seen for this instrument");
        std::println(out, "  rejected  = inputs discarded: max_ahead={}ms, max_behind={}ms",
                     Limits::max_ahead.count(), Limits::max_behind.count());
        std::println(out, "  accepted  = inputs that updated a component (received - rejected)");
        std::println(out, "  withheld  = did not produce an update: max_component_age={}ms",
                     Limits::max_component_age.count());
        std::println(out, "  queued    = updates entered the output queue");
        std::println(out, "  dropped   = updates evicted from the queue: capacity={}",
                     queue_capacity_label());
        std::println(out, "  delivered = updates consumed downstream: period={}ms, batch={}",
                     options_.consumer_period.count(), options_.consumer_batch);
    }

    const Instrument& instrument(const std::string& name) const { return instruments_.at(name); }
    const std::map<std::string, Instrument>& instruments() const { return instruments_; }
    std::uint64_t unattributed() const { return unattributed_; }
    std::size_t queued() const { return queue_.size(); }

private:
    std::optional<Reject> on_input(const Input& input, std::uint64_t seq) {
        auto* inst = lookup(input.instrument);
        if (!inst) {
            ++unattributed_;
            return Reject::too_many_instruments;
        }
        ++inst->counters.received;

        const auto verdict = validate(*inst, input);
        inst->last_seen = input.timestamp;
        if (verdict) {
            return discard(*inst, input.timestamp, *verdict);
        }

        inst->component(input.type) = Component{input.value, input.timestamp};
        ++inst->counters.accepted;
        feed_clock_ = std::max(feed_clock_.value_or(input.timestamp), input.timestamp);

        deliver_due();
        publish(input.instrument, *inst, seq);
        return std::nullopt;
    }

    Instrument* lookup(const std::string& name) {
        if (const auto it = instruments_.find(name); it != instruments_.end()) {
            return &it->second;
        }
        if (instruments_.size() >= options_.max_instruments) {
            return nullptr;
        }
        return &instruments_.try_emplace(name).first->second;
    }

    static Reject discard(Instrument& inst, std::optional<Timestamp> at, Reject reason) {
        ++inst.discards.count;
        if (at) {
            inst.discards.last = at;
        }
        return reason;
    }

    // Plausibility: references are timestamps as *seen*, accepted or not, so one bad timestamp
    // costs at most itself and its successor, and a genuine jump in time re-synchronises after a
    // single input. "Future" is judged against the newest of the instrument's last_seen and the
    // feed clock; "stale" against the instrument's own last_seen.
    std::optional<Reject> validate(const Instrument& inst, const Input& input) const {
        if (const auto own = inst.last_seen ? inst.last_seen : feed_clock_) {
            const auto newest = std::max(*own, feed_clock_.value_or(*own));
            if (input.timestamp > newest + Limits::max_ahead) {
                return Reject::future;
            }
            if (input.timestamp < *own - Limits::max_behind) {
                return Reject::stale;
            }
        }
        if (const auto& held = inst.component(input.type)) {
            if (input.timestamp < held->timestamp) {
                return Reject::superseded;
            }
            if (input.timestamp == held->timestamp && input.value == held->value) {
                return Reject::duplicate;
            }
        }
        return std::nullopt;
    }

    void publish(const std::string& name, Instrument& inst, std::uint64_t seq) {
        const auto ts = inst.state.latest_timestamp();
        if (!inst.state.ready(ts, Limits::max_component_age)) {
            ++inst.counters.withheld;
            return;
        }
        Update update{
            .timestamp  = ts,
            .seq        = seq,
            .instrument = name,
            .state      = inst.state,
            .discards   = inst.discards,
        };
        ++inst.counters.queued;
        for (auto& dropped : queue_.push(std::move(update))) {
            ++instruments_.at(dropped.instrument).counters.dropped;
        }
    }

    // The consumer runs on feed time, not wall time, so a replay is deterministic.
    void deliver_due() {
        const auto now = *feed_clock_;
        if (!next_due_) {
            next_due_ = now + options_.consumer_period;
            return;
        }
        if (now < *next_due_) {
            return;
        }
        const auto ticks = (now - *next_due_) / options_.consumer_period + 1;
        deliver(static_cast<std::size_t>(ticks) * options_.consumer_batch, now);
        *next_due_ += ticks * options_.consumer_period;
    }

    void deliver(std::size_t budget, Timestamp now) {
        while (budget > 0) {
            auto update = queue_.pop();
            if (!update) {
                return;
            }
            ++instruments_.at(update->instrument).counters.delivered;
            --budget;
            sink_(*update, now);
        }
    }

    static std::string queue_capacity_label() { return Queue::label(); }

    static std::string notes(const Instrument& inst) {
        std::string out;
        if (!inst.complete()) {
            out = "never complete (missing: " + inst.missing() + ")";
        }
        if (inst.discards.last) {
            if (!out.empty()) {
                out += "; ";
            }
            out += std::format("last rejected @{}", to_ms(*inst.discards.last));
        }
        return out;
    }

    Sink                              sink_;
    Options                           options_;
    std::map<std::string, Instrument> instruments_;
    Queue                             queue_;
    std::optional<Timestamp>          feed_clock_;
    std::optional<Timestamp>          next_due_;
    std::uint64_t                     lines_        = 0;
    std::uint64_t                     unattributed_ = 0;
};

}  // namespace test1
