#include "test1.h"

#include <fstream>
#include <iostream>

namespace {

constexpr std::string_view kCsvHeader =
    "timestamp_ms,instrument,derived_value,published_ms,"
    "discarded_count,last_discarded_ms";

std::string ms_or_empty(std::optional<test1::Timestamp> t) {
    return t ? std::format("{}", test1::to_ms(*t)) : std::string{};
}

template <class Queue>
int run(std::istream& in) {
    using namespace test1;

    std::println(std::cout, "{}", kCsvHeader);
    Publisher<DefaultLimits, Queue> publisher{[](const Update& u, Timestamp delivered_at) {
        std::println(std::cout, "{},{},{:.10g},{},{},{}", to_ms(u.timestamp),
                     u.instrument, u.derived_value(),
                     to_ms(delivered_at), u.discards.count, ms_or_empty(u.discards.last));
    }};

    std::string line;
    for (std::uint64_t line_no = 1; std::getline(in, line); ++line_no) {
        std::string_view text = line;
        if (line_no == 1 && text.starts_with("\xEF\xBB\xBF")) {
            text.remove_prefix(3);
        }
        text = trim(text);
        if (text.empty() || text.starts_with("timestamp_ms")) {
            continue;
        }
        if (const auto rejected = publisher.on_line(text, line_no)) {
            std::println(std::cerr, "line {}: rejected ({}): {}", line_no, name(*rejected), text);
        }
    }
    publisher.finish();
    publisher.print_summary(std::cerr);

    if (in.bad()) {
        std::println(std::cerr, "error: reading the input failed");
        return 1;
    }
    std::cout.flush();
    if (!std::cout) {
        std::println(std::cerr, "error: writing the output failed");
        return 1;
    }
    return 0;
}

}  // namespace

int main(int argc, char** argv) {
    std::ios::sync_with_stdio(false);
    using namespace test1;

    const auto usage = [&] {
        std::println(std::cerr, "usage: {} <file> [--queue size|time|freshest]", argv[0]);
        return 2;
    };
    if (argc < 2) {
        return usage();
    }

    std::string_view queue_mode = "size";
    int i = 2;
    while (i < argc) {
        std::string_view arg{argv[i]};
        if (arg == "--queue" && i + 1 < argc) {
            queue_mode = argv[++i];
        } else if (arg == "--freshest") {
            queue_mode = "freshest";
        } else if (arg[0] == '-') {
            return usage();
        }
        ++i;
    }

    std::ifstream in{argv[1]};
    if (!in) {
        std::println(std::cerr, "error: cannot open {}", argv[1]);
        return 1;
    }

    if (queue_mode == "size") {
        return run<SizeBoundedQueue<>>(in);
    }
    if (queue_mode == "time") {
        return run<TimeBoundedQueue<>>(in);
    }
    if (queue_mode == "freshest") {
        return run<FreshestQueue>(in);
    }
    return usage();
}
