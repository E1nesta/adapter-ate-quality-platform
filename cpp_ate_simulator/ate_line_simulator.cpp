#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace fs = std::filesystem;

struct Options {
    int count = 100;
    std::string output_dir = "data/raw";
    unsigned int seed = 20260425;
    double abnormal_rate = 0.15;
    std::string batch_no = "B20260425";
    std::string product_model = "ADP-65W";
    std::string line_id = "LINE-01";
};

struct ItemDef {
    std::string station_id;
    std::string item_name;
    double normal_mean;
    double normal_stddev;
    double lower_limit;
    double upper_limit;
    std::string unit;
    double abnormal_value;
};

std::string arg_value(int& index, int argc, char* argv[]) {
    if (index + 1 >= argc) {
        throw std::runtime_error(std::string("missing value for ") + argv[index]);
    }
    ++index;
    return argv[index];
}

Options parse_args(int argc, char* argv[]) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--count") {
            options.count = std::stoi(arg_value(i, argc, argv));
        } else if (arg == "--output-dir") {
            options.output_dir = arg_value(i, argc, argv);
        } else if (arg == "--seed") {
            options.seed = static_cast<unsigned int>(std::stoul(arg_value(i, argc, argv)));
        } else if (arg == "--abnormal-rate") {
            options.abnormal_rate = std::stod(arg_value(i, argc, argv));
        } else if (arg == "--batch-no") {
            options.batch_no = arg_value(i, argc, argv);
        } else if (arg == "--product-model") {
            options.product_model = arg_value(i, argc, argv);
        } else if (arg == "--line-id") {
            options.line_id = arg_value(i, argc, argv);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }

    if (options.count <= 0) {
        throw std::runtime_error("--count must be greater than 0");
    }
    if (options.abnormal_rate < 0.0 || options.abnormal_rate > 1.0) {
        throw std::runtime_error("--abnormal-rate must be between 0 and 1");
    }
    return options;
}

std::string timestamp_for(int product_index, int second_offset) {
    std::tm t{};
    t.tm_year = 2026 - 1900;
    t.tm_mon = 3;
    t.tm_mday = 25;
    t.tm_hour = 10;
    t.tm_min = product_index / 2;
    t.tm_sec = (product_index % 2) * 20 + second_offset;

    std::ostringstream oss;
    oss << std::put_time(&t, "%Y-%m-%d %H:%M:%S");
    return oss.str();
}

std::string format_id(const std::string& prefix, int number) {
    std::ostringstream oss;
    oss << prefix << std::setw(6) << std::setfill('0') << number;
    return oss.str();
}

double round3(double value) {
    return std::round(value * 1000.0) / 1000.0;
}

std::vector<ItemDef> item_defs() {
    return {
        {"HIPOT", "hipot_ac_withstand", 3750.0, 30.0, 3000.0, 5000.0, "V", 2600.0},
        {"HIPOT", "hipot_insulation_resistance", 220.0, 30.0, 50.0, 9999.0, "MOhm", 35.0},
        {"HIPOT", "hipot_leakage_current", 0.18, 0.05, 0.0, 0.5, "mA", 0.82},
        {"PERFORMANCE", "output_voltage", 12.0, 0.06, 11.8, 12.2, "V", 12.48},
        {"PERFORMANCE", "output_current", 2.0, 0.05, 1.8, 2.2, "A", 1.62},
        {"PERFORMANCE", "efficiency", 90.5, 0.7, 88.0, 100.0, "%", 84.2},
        {"PERFORMANCE", "ripple", 72.0, 10.0, 0.0, 120.0, "mV", 168.0},
        {"PERFORMANCE", "temperature", 45.0, 4.0, 0.0, 60.0, "C", 67.0},
        {"PERFORMANCE", "ocp_trip_current", 2.8, 0.08, 2.4, 3.2, "A", 3.45},
        {"PERFORMANCE", "scp_response_time", 22.0, 5.0, 0.0, 50.0, "ms", 72.0},
    };
}

void write_event(
    std::ofstream& events,
    int& event_seq,
    const std::string& run_id,
    const std::string& sn,
    const std::string& station_id,
    const std::string& event_type,
    const std::string& message,
    const std::string& event_time
) {
    events << format_id("EVT-", event_seq++) << ','
           << run_id << ','
           << sn << ','
           << station_id << ','
           << event_type << ','
           << message << ','
           << event_time << '\n';
}

int main(int argc, char* argv[]) {
    try {
        Options options = parse_args(argc, argv);
        fs::create_directories(options.output_dir);

        std::ofstream products(fs::path(options.output_dir) / "raw_products.csv");
        std::ofstream items(fs::path(options.output_dir) / "raw_test_items.csv");
        std::ofstream events(fs::path(options.output_dir) / "raw_station_events.csv");
        if (!products || !items || !events) {
            throw std::runtime_error("failed to open output CSV files");
        }

        products << "run_id,sn,batch_no,product_model,line_id,equipment_id,start_time,end_time,simulated_sort_result\n";
        items << "item_id,run_id,sn,station_id,item_name,measured_value,lower_limit,upper_limit,unit,test_time\n";
        events << "event_id,run_id,sn,station_id,event_type,event_message,event_time\n";

        std::mt19937 rng(options.seed);
        std::uniform_real_distribution<double> abnormal_roll(0.0, 1.0);
        std::uniform_int_distribution<int> abnormal_item_dist(0, static_cast<int>(item_defs().size()) - 1);
        int item_seq = 1;
        int event_seq = 1;
        const std::vector<ItemDef> defs = item_defs();

        products << std::fixed << std::setprecision(3);
        items << std::fixed << std::setprecision(3);

        for (int i = 0; i < options.count; ++i) {
            const std::string run_id = format_id("RUN-", i + 1);
            const std::string sn = "ADP" + format_id("", i + 1);
            const std::string equipment_id = (i % 2 == 0) ? "ATE-01" : "ATE-02";
            const bool abnormal = abnormal_roll(rng) < options.abnormal_rate;
            const int abnormal_item_index = abnormal ? abnormal_item_dist(rng) : -1;
            bool final_fail = false;

            write_event(events, event_seq, run_id, sn, "SCAN", "SCAN_OK", "SN scanned", timestamp_for(i, 0));
            write_event(events, event_seq, run_id, sn, "HIPOT", "STATION_START", "hipot station start", timestamp_for(i, 2));

            for (int j = 0; j < static_cast<int>(defs.size()); ++j) {
                const ItemDef& def = defs[j];
                std::normal_distribution<double> dist(def.normal_mean, def.normal_stddev);
                double measured = round3(dist(rng));
                if (j == abnormal_item_index) {
                    measured = def.abnormal_value;
                    final_fail = true;
                }
                if (measured < def.lower_limit || measured > def.upper_limit) {
                    final_fail = true;
                }

                items << format_id("ITEM-", item_seq++) << ','
                      << run_id << ','
                      << sn << ','
                      << def.station_id << ','
                      << def.item_name << ','
                      << measured << ','
                      << def.lower_limit << ','
                      << def.upper_limit << ','
                      << def.unit << ','
                      << timestamp_for(i, def.station_id == "HIPOT" ? 4 : 9) << '\n';

                if (j == 2) {
                    write_event(events, event_seq, run_id, sn, "HIPOT", "STATION_END", "hipot station end", timestamp_for(i, 6));
                    write_event(events, event_seq, run_id, sn, "PERFORMANCE", "STATION_START", "performance station start", timestamp_for(i, 7));
                }
            }

            write_event(events, event_seq, run_id, sn, "PERFORMANCE", "STATION_END", "performance station end", timestamp_for(i, 13));
            write_event(
                events,
                event_seq,
                run_id,
                sn,
                "SORT",
                final_fail ? "SORT_FAIL" : "SORT_PASS",
                final_fail ? "sorted to fail lane" : "sorted to pass lane",
                timestamp_for(i, 15)
            );

            products << run_id << ','
                     << sn << ','
                     << options.batch_no << ','
                     << options.product_model << ','
                     << options.line_id << ','
                     << equipment_id << ','
                     << timestamp_for(i, 0) << ','
                     << timestamp_for(i, 15) << ','
                     << (final_fail ? "FAIL" : "PASS") << '\n';
        }

        std::cout << "generated " << options.count << " product runs in " << options.output_dir << "\n";
        return 0;
    } catch (const std::exception& exc) {
        std::cerr << exc.what() << '\n';
        return 1;
    }
}
