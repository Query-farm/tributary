#include "tributary_logging.hpp"
#include "tributary_exception.hpp"
#include "duckdb.hpp"

namespace duckdb {

constexpr LogLevel TributaryLogType::LEVEL;

TributaryLogType::TributaryLogType() : LogType(NAME, LEVEL, GetLogType()) {
}

template <class ITERABLE>
static Value StringPairIterableToMap(const ITERABLE &iterable) {
	vector<Value> keys;
	vector<Value> values;
	for (const auto &kv : iterable) {
		keys.emplace_back(kv.first);
		values.emplace_back(kv.second);
	}
	return Value::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR, std::move(keys), std::move(values));
}

LogicalType TributaryLogType::GetLogType() {
	child_list_t<LogicalType> child_list = {
	    {"event", LogicalType::VARCHAR},
	    {"info", LogicalType::MAP(LogicalType::VARCHAR, LogicalType::VARCHAR)},
	};
	return LogicalType::STRUCT(child_list);
}

string TributaryLogType::ConstructLogMessage(const string &event, const vector<pair<string, string>> &info) {
	child_list_t<Value> child_list = {
	    {"event", event},
	    {"info", StringPairIterableToMap(info)},
	};

	return Value::STRUCT(std::move(child_list)).ToString();
}

#define T_LOG(SOURCE, LOG_LEVEL, ...)                                                                                  \
	DUCKDB_LOG_INTERNAL(SOURCE, TributaryLogType::NAME, LOG_LEVEL, TributaryLogType::ConstructLogMessage(__VA_ARGS__))

static LogLevel ConvertKafkaLogLevel(RdKafka::Event::Severity severity) {
	switch (severity) {
	case RdKafka::Event::EVENT_SEVERITY_EMERG:
	case RdKafka::Event::EVENT_SEVERITY_ALERT:
	case RdKafka::Event::EVENT_SEVERITY_CRITICAL:
	case RdKafka::Event::EVENT_SEVERITY_ERROR:
		return LogLevel::LOG_ERROR;
	case RdKafka::Event::EVENT_SEVERITY_WARNING:
		return LogLevel::LOG_WARNING;
	case RdKafka::Event::EVENT_SEVERITY_NOTICE:
	case RdKafka::Event::EVENT_SEVERITY_INFO:
		return LogLevel::LOG_INFO;
	case RdKafka::Event::EVENT_SEVERITY_DEBUG:
		return LogLevel::LOG_DEBUG;
	default:
		return LogLevel::LOG_INFO;
	}
}

void TributaryEventCb::event_cb(RdKafka::Event &event) {
	if (event.type() == RdKafka::Event::EVENT_LOG) {
		T_LOG(*context, ConvertKafkaLogLevel(event.severity()), event.str(),
		      {
		          {"broker", event.broker_name()},
		          {"broker_id", to_string(event.broker_id())},
		          {"facility", event.fac()},
		      });
	} else if (event.type() == RdKafka::Event::EVENT_STATS) {
		DUCKDB_LOG(*context, TributaryLogType, event.str(), {});
	} else if (event.type() == RdKafka::Event::EVENT_ERROR) {
		vector<pair<string, string>> extra = {
		    {"broker", event.broker_name()},
		    {"broker_id", to_string(event.broker_id())},
		    //		    {"broker_id", event.broker_id()},
		    //		    {"throttle_time_ms", event.throttle_time()},
		    {"fatal", event.fatal() ? "true" : "false"},
		};
		last_error_info = make_uniq<TributaryErrorInfo>(event.str(), event.fatal(), std::move(extra));
		T_LOG(*context, LogLevel::LOG_ERROR, last_error_info->msg, last_error_info->info);

		if (event.fatal()) {
			std::unordered_map<string, string> info_map;
			for (const auto &pair : last_error_info->info) {
				info_map[pair.first] = pair.second;
			}
			throw TributaryException(ExceptionType::NETWORK, event.str(), info_map);
		}
	} else if (event.type() == RdKafka::Event::EVENT_THROTTLE) {
		T_LOG(*context, LogLevel::LOG_WARNING, event.str(),
		      {
		          {"broker", event.broker_name()},
		          {"broker_id", to_string(event.broker_id())},
		          {"throttle_time_ms", to_string(event.throttle_time())},
		      });
	} else {
		T_LOG(*context, LogLevel::LOG_INFO, event.str(),
		      {
		          {"type", to_string(static_cast<int>(event.type()))},
		      });
	}
}

} // namespace duckdb
