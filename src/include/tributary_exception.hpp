#pragma once

#include "duckdb/common/exception.hpp"
#include <vector>
#include <stdexcept>

#include <librdkafka/rdkafkacpp.h>

namespace duckdb {

class TributaryException : public Exception {

private:
	static string error_message(const std::string &message, const std::string &topic, int32_t partition, int64_t offset,
	                            const std::string &broker_name, const std::string &payload_summary,
	                            const std::string &function_context);

	static unordered_map<string, string> extract_extra_info(const std::string &topic, int32_t partition, int64_t offset,
	                                                        const std::string &broker_name,
	                                                        const std::string &payload_summary,
	                                                        const std::string &function_context,
	                                                        const unordered_map<string, string> &extra_info);

public:
	explicit TributaryException(ExceptionType exception_type, const std::string &message_,
	                            const std::string &topic_ = "", int32_t partition_ = -1, int64_t offset_ = -1,
	                            const std::string &broker_name_ = "", const std::string &payload_summary_ = "",
	                            const std::string &function_context_ = "",
	                            const unordered_map<string, string> &extra_info = {})
	    : Exception(
	          extract_extra_info(topic_, partition_, offset_, broker_name_, payload_summary_, function_context_,
	                             extra_info),
	          exception_type,
	          error_message(message_, topic_, partition_, offset_, broker_name_, payload_summary_, function_context_)) {
	}

	explicit TributaryException(ExceptionType exception_type, const std::string &message_,
	                            const RdKafka::ErrorCode code_, const std::string &topic_ = "", int32_t partition_ = -1,
	                            int64_t offset_ = -1, const std::string &broker_name_ = "",
	                            const std::string &payload_summary_ = "", const std::string &function_context_ = "",
	                            const unordered_map<string, string> &extra_info = {})
	    : TributaryException(exception_type, message_ + ": " + RdKafka::err2str(code_), topic_, partition_, offset_,
	                         broker_name_, payload_summary_, function_context_, extra_info) {
	}

	explicit TributaryException(ExceptionType exception_type, const RdKafka::ErrorCode code_,
	                            const std::string &topic_ = "", int32_t partition_ = -1, int64_t offset_ = -1,
	                            const std::string &broker_name_ = "", const std::string &payload_summary_ = "",
	                            const std::string &function_context_ = "",
	                            const unordered_map<string, string> &extra_info = {})
	    : TributaryException(exception_type, RdKafka::err2str(code_), topic_, partition_, offset_, broker_name_,
	                         payload_summary_, function_context_, extra_info) {
	}

	explicit TributaryException(ExceptionType exception_type, const std::string &message_,
	                            const unordered_map<string, string> &extra_info)
	    : TributaryException(exception_type, message_, "", -1, -1, "", "", "", extra_info) {
	}
};
} // namespace duckdb