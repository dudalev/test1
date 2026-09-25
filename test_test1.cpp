// test_test1.cpp -- tests for every requirement in EXERCISE.md:
// stream parsing, derived value computation, dirty data handling,
// rate mismatch modeling, and the audit boundary.

#include "test1.h"

#include <cmath>
#include <print>
#include <sstream>
#include <vector>

namespace {

int failures = 0;

#define CHECK(condition)                                                              \
    do {                                                                              \
        if (!(condition)) {                                                           \
            ++failures;                                                               \
            std::println(stderr, "{}:{}: CHECK failed: {}", __FILE__, __LINE__, #condition); \
        }                                                                             \
    } while (0)

using namespace test1;
using namespace std::chrono_literals;

constexpr Timestamp T0 = to_timestamp(1'733'011'200'000);

bool accepted(std::optional<Reject> outcome) { return !outcome.has_value(); }
bool near(double a, double b) { return std::abs(a - b) < 1e-9; }

struct Delivered {
    Update    update;
    Timestamp at;
};

struct Collector {
    std::vector<Delivered> rows;
    Publisher<>::Sink      sink() {
        return [this](const Update& u, Timestamp at) { rows.push_back({u, at}); };
    }
};

template <class Pub>
std::optional<Reject> feed(Pub& pub, std::int64_t offset_ms, std::string_view instrument,
                           std::string_view type, double value, std::uint64_t seq) {
    return pub.on_line(std::format("{},{},{},{}", to_ms(T0) + offset_ms, instrument, type, value), seq);
}

// ---- EXERCISE: "Consume market_inputs.csv as the input stream" + "the data isn't clean" ----

void test_parse_line() {
    const auto ok = parse_line("1733011200000,ALPHA,base_rate,4.6394");
    CHECK(ok.has_value());
    CHECK(ok->timestamp == T0 && ok->instrument == "ALPHA");
    CHECK(ok->type == InputType::base_rate && ok->value == 4.6394);
    CHECK(parse_line(" 1733011200000 , ALPHA , spread , 0.05 \r").has_value());
    CHECK(parse_line("1733011200000,ALPHA,spread,-1e-3").has_value());
    CHECK(parse_line("1733011200000,ALPHA,adjustment,+0.027")->value == 0.027);
    CHECK(parse_line("1733011200000,ALPHA,adjustment,+-0.027").error().reason == Reject::bad_value);

    const auto nan = parse_line("1733011208037,ECHO,adjustment,NaN");
    CHECK(!nan.has_value() && nan.error().reason == Reject::bad_value);
    CHECK(nan.error().instrument == "ECHO" && nan.error().timestamp == to_timestamp(1733011208037));

    CHECK(parse_line("1733011211043,ECHO,spread,").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011211043,ECHO,spread,inf").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011211043,ECHO,spread,1e999").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011211043,ECHO,spread,0.1x").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,ALPHA,mid_rate,4.6").error().reason == Reject::bad_type);
    CHECK(parse_line("timestamp_ms,instrument,input_type,value").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("1733011200,ALPHA,base_rate,4.6").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("-5,ALPHA,base_rate,4.6").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("1733011200000,ALPHA,base_rate").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000,ALPHA,base_rate,4.6,extra").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000,,base_rate,4.6").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000,AL PHA,base_rate,4.6").error().reason == Reject::malformed);
    CHECK(parse_line("").error().reason == Reject::malformed);
}

// ---- EXERCISE: "the data isn't clean" -- timestamp plausibility and consistency ----

struct TightLimits {
    static constexpr Millis max_ahead         = 100ms;
    static constexpr Millis max_behind        = 50ms;
    static constexpr Millis max_component_age = 1s;
};

void test_timestamp_validation() {
    Collector out;
    Publisher<TightLimits> pub{out.sink()};
    CHECK(accepted(feed(pub, 0, "X", "base_rate", 1.0, 1)));
    CHECK(feed(pub, 101, "X", "spread", 0.1, 2) == Reject::future);
    CHECK(accepted(feed(pub, 150, "X", "spread", 0.1, 3)));
    CHECK(feed(pub, 99, "X", "adjustment", 0.0, 4) == Reject::stale);
    CHECK(accepted(feed(pub, 120, "X", "adjustment", 0.0, 5)));
    CHECK(feed(pub, 130, "X", "spread", 0.2, 6) == Reject::superseded);
    CHECK(feed(pub, 150, "X", "spread", 0.1, 7) == Reject::duplicate);
    CHECK(accepted(feed(pub, 150, "X", "spread", 0.3, 8)));

    const auto& x = pub.instrument("X");
    CHECK(x.counters.received == 8 && x.counters.accepted == 4);
    CHECK(x.discards.count == 4 && x.discards.last == T0 + 150ms);

    CHECK(feed(pub, 10'000, "Y", "base_rate", 1.0, 9) == Reject::future);
    CHECK(accepted(feed(pub, 10'050, "Y", "base_rate", 1.0, 10)));
}

// ---- EXERCISE: "Maintain current state per instrument and compute the derived value" ----

void test_derived_value() {
    Collector out;
    Publisher<> pub{out.sink()};
    CHECK(accepted(feed(pub, 0, "ALPHA", "base_rate", 4.6394, 1)));
    CHECK(accepted(feed(pub, 3, "ALPHA", "spread", 0.0588, 2)));
    CHECK(pub.queued() == 0);
    CHECK(accepted(feed(pub, 6, "ALPHA", "adjustment", -0.045, 3)));
    CHECK(accepted(feed(pub, 67, "ALPHA", "base_rate", 4.6981, 4)));
    pub.finish();

    CHECK(out.rows.size() == 2);
    const auto& first = out.rows.front().update;
    CHECK(first.instrument == "ALPHA" && first.timestamp == T0 + 6ms && first.seq == 3);
    CHECK(near(first.derived_value(), 4.6394 + 0.0588 - 0.045));
    CHECK(first.state.base_rate->value == 4.6394);
    CHECK(first.state.spread->value == 0.0588);
    CHECK(first.state.adjustment->value == -0.045);
    CHECK(near(out.rows.back().update.derived_value(), 4.6981 + 0.0588 - 0.045));
    CHECK(pub.instrument("ALPHA").counters.withheld == 2);
    CHECK(pub.instrument("ALPHA").counters.queued == 2);
}

void test_component_update_recalculates() {
    Collector out;
    Publisher<> pub{out.sink()};
    feed(pub, 0, "A", "base_rate", 1.0, 1);
    feed(pub, 1, "A", "spread", 0.5, 2);
    feed(pub, 2, "A", "adjustment", 0.1, 3);   // derived = 1.6
    feed(pub, 3, "A", "spread", 0.8, 4);        // derived = 1.0 + 0.8 + 0.1 = 1.9
    feed(pub, 4, "A", "adjustment", -0.2, 5);   // derived = 1.0 + 0.8 + (-0.2) = 1.6
    pub.finish();

    CHECK(out.rows.size() == 3);
    CHECK(near(out.rows[0].update.derived_value(), 1.6));
    CHECK(near(out.rows[1].update.derived_value(), 1.9));
    CHECK(near(out.rows[2].update.derived_value(), 1.6));
}

void test_never_complete_instrument() {
    Collector out;
    Publisher<> pub{out.sink()};
    feed(pub, 0, "D", "base_rate", 1.0, 1);
    feed(pub, 1, "D", "spread", 0.5, 2);
    feed(pub, 2, "D", "base_rate", 1.1, 3);
    feed(pub, 3, "D", "spread", 0.6, 4);
    pub.finish();

    CHECK(out.rows.empty());
    CHECK(pub.instrument("D").counters.received == 4);
    CHECK(pub.instrument("D").counters.accepted == 4);
    CHECK(pub.instrument("D").counters.withheld == 4);
    CHECK(pub.instrument("D").counters.queued == 0);
}

struct AgeLimits {
    static constexpr Millis max_ahead         = 10s;
    static constexpr Millis max_behind        = 1s;
    static constexpr Millis max_component_age = 1s;
};

void test_component_age() {
    Collector out;
    Publisher<AgeLimits> pub{out.sink()};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 0, "A", "spread", 0.5, 2);
    feed(pub, 0, "A", "adjustment", 0.25, 3);
    feed(pub, 1500, "A", "spread", 0.6, 4);
    feed(pub, 1600, "A", "base_rate", 2, 5);
    feed(pub, 1700, "A", "adjustment", 0.1, 6);
    pub.finish();
    CHECK(pub.instrument("A").counters.withheld == 4);
    CHECK(out.rows.size() == 2);
    CHECK(out.rows.back().update.seq == 6 && near(out.rows.back().update.derived_value(), 2.7));
}

// ---- EXERCISE: "Publish derived value updates to the slower consumer" ----

void test_slow_consumer() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<2>> pub{out.sink(), Options{.consumer_period = 100ms, .consumer_batch = 1}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "A", "base_rate", 2, 4);
    feed(pub, 4, "A", "base_rate", 3, 5);
    CHECK(out.rows.empty() && pub.queued() == 2);
    CHECK(pub.instrument("A").counters.dropped == 1);

    feed(pub, 100, "A", "base_rate", 4, 6);
    CHECK(out.rows.size() == 1 && out.rows.front().update.seq == 4);
    CHECK(out.rows.front().at == T0 + 100ms);

    feed(pub, 350, "A", "base_rate", 5, 7);
    CHECK(out.rows.size() == 3 && out.rows.back().update.seq == 6);

    pub.finish();
    CHECK(out.rows.size() == 4 && out.rows.back().update.seq == 7);
    CHECK(pub.instrument("A").counters.delivered == 4);
}

void test_multi_instrument_competition() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<2>> pub{out.sink(), Options{.consumer_period = 1s, .consumer_batch = 10}};

    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "B", "base_rate", 2, 4);
    feed(pub, 4, "B", "spread", 0, 5);
    feed(pub, 5, "B", "adjustment", 0, 6);

    feed(pub, 6, "A", "base_rate", 1.5, 7);
    CHECK(pub.instrument("A").counters.dropped == 1);

    pub.finish();
    CHECK(out.rows.size() == 2);
    CHECK(out.rows[0].update.seq == 6);
    CHECK(out.rows[1].update.seq == 7);
}

void test_freshest_consumer() {
    Collector out;
    Publisher<DefaultLimits, FreshestQueue> pub{out.sink(), Options{.consumer_period = 100ms, .consumer_batch = 10}};

    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "A", "base_rate", 2, 4);
    feed(pub, 4, "A", "base_rate", 3, 5);
    feed(pub, 100, "A", "base_rate", 4, 6);
    CHECK(out.rows.size() == 1 && out.rows.front().update.seq == 5);
    CHECK(pub.instrument("A").counters.dropped == 2);
    pub.finish();
    CHECK(out.rows.size() == 2 && out.rows.back().update.seq == 6);
}

void test_end_of_stream() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<10>> pub{out.sink(), Options{.consumer_period = 100ms, .consumer_batch = 1}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "A", "base_rate", 2, 4);
    feed(pub, 4, "A", "base_rate", 3, 5);
    CHECK(out.rows.empty());

    pub.finish();
    CHECK(out.rows.size() == 3);
}

// ---- EXERCISE: "what arrived, what was published, what was rejected" ----

void test_discard_tracking() {
    Collector out;
    Publisher<> pub{out.sink()};
    CHECK(pub.on_line("1733011208037,ECHO,adjustment,NaN", 1) == Reject::bad_value);
    CHECK(pub.on_line("garbage", 2) == Reject::malformed);
    CHECK(pub.on_line("1733011208040,ECHO,base_rate,4.0", 3) == std::nullopt);
    CHECK(pub.on_line("1733011208041,ECHO,spread,0.1", 4) == std::nullopt);
    CHECK(pub.on_line("1733011208042,ECHO,adjustment,0.01", 5) == std::nullopt);
    CHECK(pub.on_line("1733011208030,ECHO,adjustment,0.02", 6) == Reject::superseded);
    CHECK(pub.on_line("1733011208043,ECHO,adjustment,0.03", 7) == std::nullopt);
    pub.finish();

    const auto& echo = pub.instrument("ECHO");
    // NaN rejection (line 1) is unattributed — ECHO didn't exist yet
    CHECK(echo.counters.received == 5 && echo.counters.accepted == 4);
    CHECK(echo.discards.count == 1 && echo.discards.last == to_timestamp(1733011208030));
    CHECK(pub.unattributed() == 2);  // garbage + NaN (instrument didn't exist)

    CHECK(out.rows.size() == 2);
    CHECK(out.rows.front().update.discards.count == 0);
    CHECK(out.rows.back().update.discards.count == 1);
    CHECK(out.rows.back().update.discards.last == to_timestamp(1733011208030));

    Publisher<> tiny{out.sink(), Options{.max_instruments = 1}};
    CHECK(accepted(feed(tiny, 0, "A", "base_rate", 1, 1)));
    CHECK(feed(tiny, 1, "B", "base_rate", 1, 2) == Reject::too_many_instruments);
    CHECK(tiny.instruments().size() == 1 && tiny.unattributed() == 1);
}

// ---- EXERCISE: "how an operator would audit it later" ----

void test_audit_summary() {
    Collector out;
    Publisher<> pub{out.sink()};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    pub.on_line("1733011200010,A,spread,NaN", 4);
    pub.on_line("garbage", 5);
    pub.finish();

    std::ostringstream ss;
    pub.print_summary(ss);
    std::string summary = ss.str();
    CHECK(summary.find("5 line(s) processed") != std::string::npos);
    CHECK(summary.find("1 rejected without a usable instrument") != std::string::npos);
    // per-instrument row should show received, rejected, accepted, delivered counts
    CHECK(summary.find("4") != std::string::npos);     // received
    CHECK(summary.find("last rejected @") != std::string::npos);

    const auto& a = pub.instrument("A");
    CHECK(a.counters.received == 4);
    CHECK(a.counters.accepted == 3);
    CHECK(a.discards.count == 1);
    CHECK(a.counters.delivered == 1);
}

// ---- EXERCISE: "the data isn't clean" -- same-timestamp burst ----

void test_same_timestamp_burst() {
    // the sample's dominant dirty pattern: many updates at the same ms starve the consumer
    // because feed_clock_ doesn't advance within a burst
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<4>> pub{
        out.sink(), Options{.consumer_period = Millis{100}, .consumer_batch = 2}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0.5, 2);
    feed(pub, 2, "A", "adjustment", 0.1, 3);  // complete, queued

    // burst of 5 updates at the same timestamp — consumer can't tick within the burst
    for (int i = 0; i < 5; ++i) {
        feed(pub, 10, "A", "base_rate", 2.0 + i * 0.1, 4 + i);
    }
    CHECK(out.rows.empty());  // no tick during the burst (feed_clock_ stuck at T0+10)

    // next input at T0+110 triggers a tick
    feed(pub, 110, "A", "base_rate", 3.0, 9);
    CHECK(!out.rows.empty());
    CHECK(pub.instrument("A").counters.dropped > 0);  // some updates evicted under pressure
}

}  // namespace

int main() {
    test_parse_line();
    test_timestamp_validation();
    test_derived_value();
    test_component_update_recalculates();
    test_never_complete_instrument();
    test_component_age();
    test_slow_consumer();
    test_multi_instrument_competition();
    test_freshest_consumer();
    test_end_of_stream();
    test_discard_tracking();
    test_audit_summary();
    test_same_timestamp_burst();

    if (failures != 0) {
        std::println(stderr, "{} check(s) failed", failures);
        return 1;
    }
    std::println("all tests passed");
    return 0;
}
