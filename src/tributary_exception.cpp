#include "duckdb.hpp"
#include "tributary_exception.hpp"

#include "duckdb.hpp"

using namespace duckdb_yyjson; // NOLINT

namespace duckdb {

unordered_map<string, string> TributaryException::extract_extra_info(const std::string &topic, int32_t partition,
                                                                     int64_t offset, const std::string &broker_name,
                                                                     const std::string &payload_summary,
                                                                     const std::string &function_context,
                                                                     const unordered_map<string, string> &extra_info) {
	std::unordered_map<std::string, std::string> result = extra_info;
	if (!topic.empty()) {
		result["topic"] = topic;
	}
	if (partition != -1) {
		result["partition"] = std::to_string(partition);
	}
	if (offset != -1) {
		result["offset"] = std::to_string(offset);
	}
	if (!broker_name.empty()) {
		result["broker_name"] = broker_name;
	}
	if (!payload_summary.empty()) {
		result["payload_summary"] = payload_summary;
	}
	if (!function_context.empty()) {
		result["function_context"] = function_context;
	}

	return result;
}

string TributaryException::error_message(const std::string &message, const std::string &topic, int32_t partition,
                                         int64_t offset, const std::string &broker_name,
                                         const std::string &payload_summary, const std::string &function_context) {
	string result = "Error: " + message;

	if (!topic.empty()) {
		result += " topic='" + topic + "'";
	}
	if (partition != -1) {
		result += ", partition=" + std::to_string(partition);
	}
	if (offset != -1) {
		result += ", offset=" + std::to_string(offset);
	}
	if (!broker_name.empty()) {
		result += " (broker: " + broker_name + ")";
	}
	if (!payload_summary.empty()) {
		result += ", payload: " + payload_summary;
	}
	if (!function_context.empty()) {
		result += ", context: " + function_context;
	}
	return result;
}

} // namespace duckdb