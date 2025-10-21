#pragma once

#include "duckdb/logging/logging.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb.hpp"
#include <librdkafka/rdkafkacpp.h>

namespace duckdb {

struct TributaryLogType : public LogType {
	static constexpr const char *NAME = "Tributary";
	static constexpr LogLevel LEVEL = LogLevel::LOG_INFO;

	//! Construct the log type
	TributaryLogType();

	static LogicalType GetLogType();

	static string ConstructLogMessage(const string &event, const vector<pair<string, string>> &info);
};

class TributaryErrorInfo {
public:
	const string msg;
	const vector<std::pair<string, string>> info;
	const bool fatal = false;

	explicit TributaryErrorInfo(const string &last_error_, bool last_error_fatal_,
	                            const vector<std::pair<string, string>> &last_error_info_)
	    : msg(std::move(last_error_)), info(std::move(last_error_info_)), fatal(last_error_fatal_) {
	}
};

class TributaryEventCb : public RdKafka::EventCb {
private:
	shared_ptr<ClientContext> context;
	unique_ptr<TributaryErrorInfo> last_error_info = nullptr;

public:
	explicit TributaryEventCb(shared_ptr<ClientContext> context) : context(std::move(context)) {
	}

	void reset() {
		last_error_info = nullptr;
	}

	void event_cb(RdKafka::Event &event) override;
};

} // namespace duckdb
