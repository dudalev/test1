// test_private_test1.cpp -- edge-case and internal tests beyond the EXERCISE requirements.

#include "test1.h"

#include <cmath>
#include <limits>
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
    Publisher<>::Sink sink() {
        return [this](const Update& u, Timestamp at) { rows.push_back({u, at}); };
    }
};

template <class Pub>
std::optional<Reject> feed(Pub& pub, std::int64_t offset_ms, std::string_view instrument,
                           std::string_view type, double value, std::uint64_t seq) {
    return pub.on_line(std::format("{},{},{},{}", to_ms(T0) + offset_ms, instrument, type, value), seq);
}

Update make_update(std::int64_t offset_ms, std::uint64_t seq) {
    return Update{.timestamp = T0 + Millis{offset_ms}, .seq = seq, .instrument = "A"};
}

// ---- parse_number internals ----

void test_parse_number_integers() {
    CHECK(parse_number<std::int64_t>("0") == 0);
    CHECK(parse_number<std::int64_t>("-1") == -1);
    CHECK(parse_number<std::int64_t>("+42") == 42);
    CHECK(parse_number<std::int64_t>("007") == 7);
    CHECK(!parse_number<std::int64_t>(""));
    CHECK(!parse_number<std::int64_t>("+"));
    CHECK(!parse_number<std::int64_t>("-"));
    CHECK(!parse_number<std::int64_t>("+-1"));
    CHECK(!parse_number<std::int64_t>("1.5"));
    CHECK(!parse_number<std::int64_t>("0xFF"));
    CHECK(!parse_number<std::int64_t>("1e3"));
    CHECK(!parse_number<std::int64_t>("12 "));
    CHECK(!parse_number<std::int64_t>(" 12"));
    CHECK(!parse_number<std::int64_t>("12abc"));
}

void test_parse_number_doubles() {
    CHECK(near(*parse_number<double>("3.14"), 3.14));
    CHECK(near(*parse_number<double>("-0.5"), -0.5));
    CHECK(near(*parse_number<double>("+2.718"), 2.718));
    CHECK(near(*parse_number<double>("1e-3"), 0.001));
    CHECK(near(*parse_number<double>("-1e2"), -100.0));
    CHECK(near(*parse_number<double>("1."), 1.0));
    CHECK(near(*parse_number<double>(".5"), 0.5));
    CHECK(near(*parse_number<double>("0"), 0.0));
    CHECK(!parse_number<double>(""));
    CHECK(parse_number<double>("NaN").has_value() && std::isnan(*parse_number<double>("NaN")));
    CHECK(parse_number<double>("inf").has_value() && std::isinf(*parse_number<double>("inf")));
    CHECK(parse_number<double>("-inf").has_value() && std::isinf(*parse_number<double>("-inf")));
    CHECK(!parse_number<double>("1e999"));
    CHECK(!parse_number<double>("abc"));
    CHECK(!parse_number<double>("1.2.3"));
    CHECK(!parse_number<double>("1,5"));
}

// ---- parse_line edge cases ----

void test_parse_line_field_counts() {
    CHECK(parse_line("").error().reason == Reject::malformed);
    CHECK(parse_line(",,,").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000,A,spread").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000,A,spread,1,x").error().reason == Reject::malformed);
    CHECK(parse_line("no commas at all").error().reason == Reject::malformed);
    CHECK(parse_line(",").error().reason == Reject::malformed);
    CHECK(parse_line(",,").error().reason == Reject::malformed);
}

void test_parse_line_whitespace() {
    auto ok = parse_line("  1733011200000 , ALPHA , base_rate , 4.5  ");
    CHECK(ok.has_value());
    CHECK(ok->instrument == "ALPHA" && near(ok->value, 4.5));

    ok = parse_line("\t1733011200000\t,\tALPHA\t,\tspread\t,\t0.1\t");
    CHECK(ok.has_value());

    ok = parse_line("1733011200000,ALPHA,adjustment,0.02\r");
    CHECK(ok.has_value());
}

void test_parse_line_instruments() {
    CHECK(parse_line("1733011200000,,spread,1").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000, ,spread,1").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000,A B,spread,1").error().reason == Reject::malformed);
    CHECK(parse_line("1733011200000,A\tB,spread,1").error().reason == Reject::malformed);

    std::string long32(32, 'X');
    CHECK(parse_line(std::format("1733011200000,{},spread,1.0", long32)).has_value());

    std::string long33(33, 'X');
    CHECK(parse_line(std::format("1733011200000,{},spread,1.0", long33)).error().reason == Reject::malformed);

    CHECK(parse_line("1733011200000,Z,spread,1.0").has_value());
    CHECK(parse_line("1733011200000,INST_42,spread,1.0").has_value());
    CHECK(parse_line("1733011200000,A.B-C,spread,1.0").has_value());
}

void test_parse_line_timestamp_bounds() {
    CHECK(parse_line("946684800000,A,spread,1.0").has_value());
    CHECK(parse_line("946684799999,A,spread,1.0").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("4102444800000,A,spread,1.0").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("4102444799999,A,spread,1.0").has_value());
    CHECK(parse_line("-1,A,spread,1.0").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("0,A,spread,1.0").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("1733011200,A,spread,1.0").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("abc,A,spread,1.0").error().reason == Reject::bad_timestamp);
    CHECK(parse_line("17330112000x0,A,spread,1.0").error().reason == Reject::bad_timestamp);
}

void test_parse_line_types() {
    CHECK(parse_line("1733011200000,A,base_rate,1")->type == InputType::base_rate);
    CHECK(parse_line("1733011200000,A,spread,1")->type == InputType::spread);
    CHECK(parse_line("1733011200000,A,adjustment,1")->type == InputType::adjustment);
    CHECK(parse_line("1733011200000,A,Base_Rate,1").error().reason == Reject::bad_type);
    CHECK(parse_line("1733011200000,A,SPREAD,1").error().reason == Reject::bad_type);
    CHECK(parse_line("1733011200000,A,ADJUSTMENT,1").error().reason == Reject::bad_type);
    CHECK(parse_line("1733011200000,A,mid_rate,1").error().reason == Reject::bad_type);
    CHECK(parse_line("1733011200000,A,rate,1").error().reason == Reject::bad_type);
    CHECK(parse_line("1733011200000,A,,1").error().reason == Reject::bad_type);
}

void test_parse_line_values() {
    CHECK(near(parse_line("1733011200000,A,spread,0")->value, 0.0));
    CHECK(near(parse_line("1733011200000,A,spread,-1.0")->value, -1.0));
    CHECK(near(parse_line("1733011200000,A,spread,+0.027")->value, 0.027));
    CHECK(near(parse_line("1733011200000,A,spread,1e-3")->value, 0.001));
    CHECK(near(parse_line("1733011200000,A,spread,.5")->value, 0.5));
    CHECK(parse_line("1733011200000,A,spread,").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,A,spread,NaN").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,A,spread,inf").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,A,spread,-inf").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,A,spread,1e999").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,A,spread,abc").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,A,spread,1.0x").error().reason == Reject::bad_value);
    CHECK(parse_line("1733011200000,A,spread,+-1").error().reason == Reject::bad_value);
}

void test_parse_error_attribution() {
    auto err = parse_line("1733011200000,ECHO,spread,NaN").error();
    CHECK(err.reason == Reject::bad_value && err.instrument == "ECHO");
    CHECK(err.timestamp == to_timestamp(1733011200000));

    err = parse_line("1733011200000,ALPHA,nonsense,1.0").error();
    CHECK(err.reason == Reject::bad_type && err.instrument == "ALPHA");

    err = parse_line("abc,BRAVO,spread,1.0").error();
    CHECK(err.reason == Reject::bad_timestamp && err.instrument == "BRAVO" && !err.timestamp.has_value());

    err = parse_line("garbage").error();
    CHECK(err.reason == Reject::malformed && err.instrument.empty());
}

// ---- instrument state helpers ----

void test_instrument_missing() {
    Instrument inst{};
    CHECK(inst.missing() == "base_rate+spread+adjustment");
    CHECK(!inst.complete());

    inst.state.base_rate = Component{1.0, T0};
    CHECK(inst.missing() == "spread+adjustment");

    inst.state.spread = Component{0.5, T0};
    CHECK(inst.missing() == "adjustment");

    inst.state.adjustment = Component{0.1, T0};
    CHECK(inst.missing().empty());
    CHECK(inst.complete());
}

// ---- validation boundary tests ----

struct ExactLimits {
    static constexpr Millis max_ahead         = 100ms;
    static constexpr Millis max_behind        = 50ms;
    static constexpr Millis max_component_age = 10s;
};

void test_validation_exact_ahead_boundary() {
    Collector out;
    Publisher<ExactLimits> pub{out.sink()};
    CHECK(accepted(feed(pub, 0, "A", "base_rate", 1.0, 1)));
    CHECK(accepted(feed(pub, 100, "A", "spread", 0.1, 2)));
    CHECK(feed(pub, 201, "A", "adjustment", 0.0, 3) == Reject::future);
}

void test_validation_exact_behind_boundary() {
    Collector out;
    Publisher<ExactLimits> pub{out.sink()};
    CHECK(accepted(feed(pub, 1000, "A", "base_rate", 1.0, 1)));
    CHECK(accepted(feed(pub, 950, "A", "spread", 0.1, 2)));
    CHECK(feed(pub, 899, "A", "adjustment", 0.0, 3) == Reject::stale);
}

void test_validation_resync_after_future() {
    Collector out;
    Publisher<ExactLimits> pub{out.sink()};
    CHECK(accepted(feed(pub, 0, "A", "base_rate", 1.0, 1)));
    CHECK(feed(pub, 200, "A", "spread", 0.1, 2) == Reject::future);
    CHECK(accepted(feed(pub, 250, "A", "spread", 0.1, 3)));
}

void test_feed_clock_cross_instrument() {
    Collector out;
    Publisher<ExactLimits> pub{out.sink()};
    CHECK(accepted(feed(pub, 1000, "A", "base_rate", 1.0, 1)));
    CHECK(accepted(feed(pub, 1100, "B", "base_rate", 2.0, 2)));
    CHECK(feed(pub, 1201, "C", "base_rate", 3.0, 3) == Reject::future);
}

void test_feed_clock_only_advances() {
    Collector out;
    Publisher<ExactLimits> pub{out.sink()};
    CHECK(accepted(feed(pub, 1000, "A", "base_rate", 1.0, 1)));
    CHECK(accepted(feed(pub, 950, "A", "spread", 0.1, 2)));
    CHECK(accepted(feed(pub, 1100, "B", "base_rate", 2.0, 3)));
}

void test_superseded_and_duplicate() {
    Collector out;
    Publisher<> pub{out.sink()};
    CHECK(accepted(feed(pub, 0, "A", "base_rate", 1.0, 1)));
    CHECK(accepted(feed(pub, 1, "A", "spread", 0.5, 2)));
    CHECK(accepted(feed(pub, 2, "A", "adjustment", 0.1, 3)));
    CHECK(feed(pub, 2, "A", "adjustment", 0.1, 4) == Reject::duplicate);
    CHECK(accepted(feed(pub, 2, "A", "adjustment", 0.2, 5)));
    CHECK(accepted(feed(pub, 1, "A", "base_rate", 1.5, 6)));
    CHECK(feed(pub, 0, "A", "base_rate", 2.0, 7) == Reject::superseded);
}

// ---- component age boundary ----

struct ShortAgeLimits {
    static constexpr Millis max_ahead         = 10s;
    static constexpr Millis max_behind        = 1s;
    static constexpr Millis max_component_age = 500ms;
};

void test_component_age_boundary() {
    Collector out;
    Publisher<ShortAgeLimits> pub{out.sink()};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 0, "A", "spread", 0.5, 2);
    feed(pub, 0, "A", "adjustment", 0.25, 3);
    CHECK(pub.instrument("A").counters.queued == 1);

    feed(pub, 500, "A", "spread", 0.6, 4);
    CHECK(pub.instrument("A").counters.queued == 2);

    feed(pub, 501, "A", "spread", 0.7, 5);
    CHECK(pub.instrument("A").counters.withheld == 3);

    feed(pub, 502, "A", "base_rate", 2, 6);
    CHECK(pub.instrument("A").counters.withheld == 4);

    feed(pub, 503, "A", "adjustment", 0.1, 7);
    CHECK(pub.instrument("A").counters.queued == 3);
}

// ---- queue internals ----

void test_queue_ordering() {
    SizeBoundedQueue<3> q;
    CHECK(q.push(make_update(30, 3)).empty());
    CHECK(q.push(make_update(10, 1)).empty());
    CHECK(q.push(make_update(20, 2)).empty());
    auto evicted = q.push(make_update(40, 4));
    CHECK(evicted.size() == 1 && evicted[0].seq == 1);
    CHECK(q.pop()->seq == 2 && q.pop()->seq == 3 && q.pop()->seq == 4);
    CHECK(!q.pop());

    FreshestQueue fq;
    CHECK(fq.push(make_update(30, 3)).empty());
    evicted = fq.push(make_update(10, 1));       // older: evicted immediately, 30 stays
    CHECK(evicted.size() == 1 && evicted[0].seq == 1);
    CHECK(fq.pop()->seq == 3);
    CHECK(!fq.pop());
}

void test_queue_capacity_one() {
    SizeBoundedQueue<1> q;
    CHECK(q.push(make_update(10, 1)).empty());
    auto evicted = q.push(make_update(20, 2));
    CHECK(evicted.size() == 1 && evicted[0].seq == 1);
    CHECK(q.size() == 1);
    CHECK(q.pop()->seq == 2);
    CHECK(q.empty());

    FreshestQueue fq;
    CHECK(fq.push(make_update(10, 1)).empty());
    evicted = fq.push(make_update(20, 2));
    CHECK(evicted.size() == 1 && evicted[0].seq == 1);
    CHECK(fq.pop()->seq == 2);
}

void test_queue_empty_operations() {
    SizeBoundedQueue<5> q;
    CHECK(q.empty());
    CHECK(q.size() == 0);
    CHECK(!q.pop());
    CHECK(!q.pop());
}

void test_queue_interleaved_push_pop() {
    SizeBoundedQueue<10> q;
    q.push(make_update(30, 3));
    q.push(make_update(10, 1));
    CHECK(q.pop()->seq == 1);
    q.push(make_update(5, 4));
    CHECK(q.pop()->seq == 4);
    CHECK(q.pop()->seq == 3);
    CHECK(q.empty());
}

void test_queue_same_timestamp_ordering() {
    SizeBoundedQueue<10> q;
    q.push(make_update(100, 5));
    q.push(make_update(100, 3));
    q.push(make_update(100, 7));
    CHECK(q.pop()->seq == 3);
    CHECK(q.pop()->seq == 5);
    CHECK(q.pop()->seq == 7);
}

void test_queue_eviction_under_load() {
    SizeBoundedQueue<3> q;
    for (int i = 1; i <= 3; ++i) {
        q.push(make_update(i * 10, i));
    }
    auto evicted = q.push(make_update(40, 4));
    CHECK(evicted.size() == 1 && evicted[0].seq == 1);

    evicted = q.push(make_update(5, 5));
    CHECK(evicted.size() == 1 && evicted[0].seq == 5);
    CHECK(q.pop()->seq == 2);

    FreshestQueue fq;
    fq.push(make_update(30, 3));
    fq.push(make_update(10, 1));   // older: evicted, 30 stays
    fq.push(make_update(20, 2));   // older: evicted, 30 stays
    evicted = fq.push(make_update(40, 4));  // newer: replaces 30
    CHECK(evicted.size() == 1 && evicted[0].seq == 3);
    CHECK(fq.pop()->seq == 4);
}

void test_time_bounded_queue() {
    TimeBoundedQueue<100, 1000> q;
    q.push(make_update(0, 1));
    q.push(make_update(50, 2));
    CHECK(q.size() == 2);

    auto evicted = q.push(make_update(101, 3));
    CHECK(evicted.size() == 1 && evicted[0].seq == 1);
    CHECK(q.size() == 2);

    evicted = q.push(make_update(200, 4));
    CHECK(evicted.size() == 1 && evicted[0].seq == 2);
    CHECK(q.size() == 2);

    CHECK(q.pop()->seq == 3);
    CHECK(q.pop()->seq == 4);
}

// ---- consumer mechanics ----

void test_consumer_batch_limiting() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<10>> pub{out.sink(), Options{.consumer_period = 100ms, .consumer_batch = 2}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "A", "base_rate", 2, 4);
    feed(pub, 4, "A", "base_rate", 3, 5);
    feed(pub, 5, "A", "base_rate", 4, 6);
    CHECK(pub.queued() == 4);

    feed(pub, 100, "A", "base_rate", 5, 7);
    CHECK(out.rows.size() == 2);
    CHECK(out.rows[0].update.seq == 3);
    CHECK(out.rows[1].update.seq == 4);
}

void test_consumer_multiple_periods_elapsed() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<20>> pub{out.sink(), Options{.consumer_period = 100ms, .consumer_batch = 1}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "A", "base_rate", 2, 4);
    feed(pub, 4, "A", "base_rate", 3, 5);
    feed(pub, 5, "A", "base_rate", 4, 6);
    CHECK(pub.queued() == 4);

    feed(pub, 350, "A", "base_rate", 5, 7);
    CHECK(out.rows.size() == 3);
}

void test_consumer_finish_with_no_inputs() {
    Collector out;
    Publisher<> pub{out.sink()};
    pub.finish();
    CHECK(out.rows.empty());
}

void test_consumer_period_clamped_to_1ms() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<10>> pub{out.sink(), Options{.consumer_period = 0ms, .consumer_batch = 1}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "A", "base_rate", 2, 4);
    CHECK(out.rows.size() == 1);
}

void test_freshest_queue_multi_instrument() {
    Collector out;
    Publisher<DefaultLimits, FreshestQueue> pub{out.sink(), Options{.consumer_period = 200ms, .consumer_batch = 10}};

    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 3, "B", "base_rate", 2, 4);
    feed(pub, 4, "B", "spread", 0, 5);
    feed(pub, 5, "B", "adjustment", 0, 6);
    feed(pub, 10, "A", "base_rate", 1.5, 7);

    feed(pub, 200, "A", "base_rate", 1.6, 9);
    CHECK(out.rows.size() == 1);
    CHECK(out.rows[0].update.seq == 7);
    CHECK(pub.instrument("A").counters.dropped == 1);
    CHECK(pub.instrument("B").counters.dropped == 1);
}

// ---- discard tracking detail ----

void test_discard_stats_last_timestamp_update() {
    Collector out;
    Publisher<ExactLimits> pub{out.sink()};

    feed(pub, 0, "A", "base_rate", 1.0, 1);
    CHECK(pub.instrument("A").discards.count == 0);

    CHECK(feed(pub, 200, "A", "spread", 0.1, 2) == Reject::future);
    CHECK(pub.instrument("A").discards.count == 1);
    CHECK(pub.instrument("A").discards.last == T0 + 200ms);

    CHECK(feed(pub, 301, "A", "adjustment", 0.0, 3) == Reject::future);
    CHECK(pub.instrument("A").discards.count == 2);
    CHECK(pub.instrument("A").discards.last == T0 + 301ms);
}

void test_discard_stats_in_delivered_updates() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<10>> pub{out.sink(), Options{.consumer_period = Millis{1}, .consumer_batch = 10}};

    // NaN before instrument exists → unattributed
    pub.on_line("1733011200000,X,adjustment,NaN", 1);
    CHECK(pub.unattributed() == 1);

    feed(pub, 1, "X", "base_rate", 1.0, 2);
    feed(pub, 2, "X", "spread", 0.5, 3);
    feed(pub, 3, "X", "adjustment", 0.1, 4);  // complete, discards.count == 0

    // inf after instrument exists → attributed
    pub.on_line("1733011200005,X,spread,inf", 5);
    CHECK(pub.instrument("X").discards.count == 1);
    feed(pub, 6, "X", "base_rate", 1.1, 6);

    pub.finish();
    CHECK(out.rows.size() == 2);
    CHECK(out.rows[0].update.discards.count == 0);
    CHECK(out.rows[1].update.discards.count == 1);
    CHECK(out.rows[1].update.discards.last == to_timestamp(1733011200005));
}

// ---- on_line attribution ----

void test_on_line_unattributed_rejections() {
    Collector out;
    Publisher<> pub{out.sink()};
    pub.on_line("garbage", 1);
    pub.on_line("", 2);
    pub.on_line(",,,", 3);
    CHECK(pub.unattributed() == 3);

    pub.on_line("1733011200000,KNOWN,spread", 4);
    CHECK(pub.unattributed() == 4);
}

void test_on_line_attributed_rejections() {
    Collector out;
    Publisher<> pub{out.sink()};
    // first create the instrument with a valid input
    pub.on_line("1733011200000,ECHO,base_rate,4.0", 1);
    // now a rejection for the same instrument is attributed
    pub.on_line("1733011200001,ECHO,adjustment,NaN", 2);
    CHECK(pub.instrument("ECHO").discards.count == 1);
    CHECK(pub.instrument("ECHO").counters.received == 2);
    CHECK(pub.unattributed() == 0);
}

// ---- max_instruments ----

void test_max_instruments() {
    Collector out;
    Publisher<> pub{out.sink(), Options{.max_instruments = 3}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "B", "base_rate", 1, 2);
    feed(pub, 2, "C", "base_rate", 1, 3);
    CHECK(feed(pub, 3, "D", "base_rate", 1, 4) == Reject::too_many_instruments);
    CHECK(pub.unattributed() == 1);
    CHECK(pub.instruments().size() == 3);
    CHECK(accepted(feed(pub, 4, "A", "spread", 0.5, 5)));
}

// ---- derived value edge cases ----

void test_derived_value_with_negatives() {
    Collector out;
    Publisher<> pub{out.sink()};
    feed(pub, 0, "A", "base_rate", -1.0, 1);
    feed(pub, 1, "A", "spread", 0.5, 2);
    feed(pub, 2, "A", "adjustment", -0.25, 3);
    pub.finish();
    CHECK(out.rows.size() == 1);
    CHECK(near(out.rows[0].update.derived_value(), -0.75));
}

void test_derived_value_all_zero() {
    Collector out;
    Publisher<> pub{out.sink()};
    feed(pub, 0, "A", "base_rate", 0.0, 1);
    feed(pub, 1, "A", "spread", 0.0, 2);
    feed(pub, 2, "A", "adjustment", 0.0, 3);
    pub.finish();
    CHECK(out.rows.size() == 1);
    CHECK(out.rows[0].update.derived_value() == 0.0);
}

// ---- delivered_at timestamp ----

void test_delivered_at_timestamp() {
    Collector out;
    Publisher<DefaultLimits, SizeBoundedQueue<10>> pub{out.sink(), Options{.consumer_period = 100ms, .consumer_batch = 10}};
    feed(pub, 0, "A", "base_rate", 1, 1);
    feed(pub, 1, "A", "spread", 0, 2);
    feed(pub, 2, "A", "adjustment", 0, 3);
    feed(pub, 150, "A", "base_rate", 2, 4);
    CHECK(out.rows.size() == 1);
    CHECK(out.rows[0].at == T0 + 150ms);
    CHECK(out.rows[0].update.timestamp == T0 + 2ms);

    pub.finish();
    CHECK(out.rows.size() == 2);
    CHECK(out.rows[1].at == T0 + 150ms);
}

// ---- custom limits policy ----

struct ZeroToleranceLimits {
    static constexpr Millis max_ahead         = 0ms;
    static constexpr Millis max_behind        = 0ms;
    static constexpr Millis max_component_age = 10s;
};

void test_zero_tolerance_limits() {
    Collector out;
    Publisher<ZeroToleranceLimits> pub{out.sink()};
    CHECK(accepted(feed(pub, 0, "A", "base_rate", 1.0, 1)));
    CHECK(feed(pub, 1, "A", "spread", 0.1, 2) == Reject::future);
    CHECK(accepted(feed(pub, 1, "A", "spread", 0.1, 3)));
    CHECK(feed(pub, 0, "A", "adjustment", 0.0, 4) == Reject::stale);
    CHECK(accepted(feed(pub, 1, "A", "adjustment", 0.0, 5)));
}

}  // namespace

int main() {
    test_parse_number_integers();
    test_parse_number_doubles();
    test_parse_line_field_counts();
    test_parse_line_whitespace();
    test_parse_line_instruments();
    test_parse_line_timestamp_bounds();
    test_parse_line_types();
    test_parse_line_values();
    test_parse_error_attribution();
    test_instrument_missing();
    test_validation_exact_ahead_boundary();
    test_validation_exact_behind_boundary();
    test_validation_resync_after_future();
    test_feed_clock_cross_instrument();
    test_feed_clock_only_advances();
    test_superseded_and_duplicate();
    test_component_age_boundary();
    test_queue_ordering();
    test_queue_capacity_one();
    test_queue_empty_operations();
    test_queue_interleaved_push_pop();
    test_queue_same_timestamp_ordering();
    test_queue_eviction_under_load();
    test_time_bounded_queue();
    test_consumer_batch_limiting();
    test_consumer_multiple_periods_elapsed();
    test_consumer_finish_with_no_inputs();
    test_consumer_period_clamped_to_1ms();
    test_freshest_queue_multi_instrument();
    test_discard_stats_last_timestamp_update();
    test_discard_stats_in_delivered_updates();
    test_on_line_unattributed_rejections();
    test_on_line_attributed_rejections();
    test_max_instruments();
    test_derived_value_with_negatives();
    test_derived_value_all_zero();
    test_delivered_at_timestamp();
    test_zero_tolerance_limits();

    if (failures != 0) {
        std::println(stderr, "{} check(s) failed", failures);
        return 1;
    }
    std::println("all {} tests passed", 38);
    return 0;
}
