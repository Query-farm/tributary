#include "tributary_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <librdkafka/rdkafkacpp.h>
#include "tributary_config.hpp"
#include <optional>

namespace duckdb {

struct TributaryScanTopicPartitionTask {
	int32_t partition_;
	int64_t low_;
	int64_t high_;

	TributaryScanTopicPartitionTask(int32_t p, int64_t low, int64_t high) : partition_(p), low_(low), high_(high) {
	}
};

struct TributaryScanTopicBindData : public TableFunctionData {
	std::unique_ptr<RdKafka::Conf> config;

	const std::string topic;

	explicit TributaryScanTopicBindData(std::string topic, std::unordered_map<string, string> config_values)
	    : topic(std::move(topic)) {

		config.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

		for (auto &kv : config_values) {
			auto &key = kv.first;
			auto &value = kv.second;

			std::string errstr;
			if (config->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
				throw InvalidConfigurationException("Failed to set " + key + ": " + errstr);
			}
		}
	}
};

struct TributaryScanTopicGlobalState : public GlobalTableFunctionState {
	// We need to get a list of the watermarks for each partition in the topic.
public:
	explicit TributaryScanTopicGlobalState(std::vector<TributaryScanTopicPartitionTask> &tasks)
	    : GlobalTableFunctionState() {
		for (auto &task : tasks) {
			task_queue_.push(task);
		}
	}

	idx_t MaxThreads() const override {
		return std::min(task_queue_.size(), (size_t)1);
	}

	std::optional<TributaryScanTopicPartitionTask> obtain_task() {
		std::lock_guard<std::mutex> lock(queue_mutex_);
		if (task_queue_.empty()) {
			return std::nullopt;
		}
		auto task = task_queue_.front();
		task_queue_.pop();
		return task;
	}

private:
	std::queue<TributaryScanTopicPartitionTask> task_queue_;
	std::mutex queue_mutex_;
};

struct TributaryScanTopicLocalState : public LocalTableFunctionState {

	std::optional<TributaryScanTopicPartitionTask> current_task_ = std::nullopt;
	std::unique_ptr<RdKafka::Consumer> consumer = nullptr;
	std::unique_ptr<RdKafka::Topic> topic = nullptr;

	explicit TributaryScanTopicLocalState() : LocalTableFunctionState() {
	}

	std::optional<TributaryScanTopicPartitionTask>
	obtain_partition_scan_task(TributaryScanTopicBindData &bind_data, TributaryScanTopicGlobalState &global_state) {
		if (current_task_.has_value()) {
			return current_task_;
		}

		auto task = global_state.obtain_task();
		if (!task.has_value()) {
			// No more tasks available
			return std::nullopt;
		}

		std::string errstr;
		consumer = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(bind_data.config.get(), errstr));
		if (!consumer) {
			throw InternalException("Failed to create consumer: " + errstr);
		}

		std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
		topic = std::unique_ptr<RdKafka::Topic>(
		    RdKafka::Topic::create(consumer.get(), bind_data.topic, tconf.get(), errstr));

		if (!topic) {
			throw InternalException("Failed to create topic: " + errstr);
		}

		RdKafka::ErrorCode err = consumer->start(topic.get(), task->partition_, RdKafka::Topic::OFFSET_BEGINNING);
		if (err != RdKafka::ERR_NO_ERROR) {
			throw InternalException("Failed to start consuming topic " + bind_data.topic + " partition " +
			                        std::to_string(task->partition_) + ": " + RdKafka::err2str(err));
		}
		current_task_ = task;
		return current_task_;
	}

	void finish_partition_scan() {
		if (current_task_.has_value()) {
			// We are done with this task, so we can reset it.
			consumer->stop(topic.get(), current_task_->partition_);

			current_task_ = std::nullopt;
		}
	}
};

static unique_ptr<FunctionData> TributaryScanTopicBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {

	auto topic = input.inputs[0].GetValue<string>();

	std::unordered_map<string, string> config_values;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		config_values[loption] = StringValue::Get(kv.second);
	}

	names.push_back("topic");
	names.push_back("partition");
	names.push_back("offset");
	names.push_back("message");
	return_types.push_back(LogicalType(LogicalTypeId::VARCHAR));
	return_types.push_back(LogicalType(LogicalTypeId::INTEGER));
	return_types.push_back(LogicalType(LogicalTypeId::BIGINT));
	return_types.push_back(LogicalType(LogicalTypeId::BLOB));

	return make_uniq<TributaryScanTopicBindData>(std::move(topic), std::move(config_values));
}

static unique_ptr<GlobalTableFunctionState> TributaryScanTopicGlobalInit(ClientContext &context,
                                                                         TableFunctionInitInput &input) {

	auto &bind_data = input.bind_data->Cast<TributaryScanTopicBindData>();

	std::string errstr;
	auto consumer = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(bind_data.config.get(), errstr));

	if (!consumer) {
		throw InternalException("Failed to create consumer: " + errstr);
	}

	// FIXME: allow additional config to be specified here.
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	auto topic =
	    std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(consumer.get(), bind_data.topic, tconf, errstr));

	if (!topic) {
		throw InternalException("Failed to create topic: " + errstr);
	}

	RdKafka::Metadata *metadata;
	RdKafka::ErrorCode err = consumer->metadata(false, topic.get(), &metadata, 5000);
	if (err != RdKafka::ERR_NO_ERROR) {
		throw InternalException("Failed to get topic metadata: " + RdKafka::err2str(err));
	}

	const RdKafka::TopicMetadata *topic_metadata = nullptr;
	auto topics = metadata->topics();
	for (auto it = topics->begin(); it != topics->end(); ++it) {
		if ((*it)->topic() == bind_data.topic) {
			topic_metadata = *it;
			break;
		}
	}

	if (!topic_metadata) {
		throw InternalException("Topic not found: " + bind_data.topic);
		delete metadata;
	}

	std::vector<TributaryScanTopicPartitionTask> partition_tasks;
	// Get partitions and their watermarks
	auto partitions = topic_metadata->partitions();

	for (auto it = partitions->begin(); it != partitions->end(); ++it) {
		int32_t partition = (*it)->id();
		int64_t low, high;

		err = consumer->query_watermark_offsets(bind_data.topic, partition, &low, &high, 5000);
		if (err == RdKafka::ERR_NO_ERROR) {
			partition_tasks.emplace_back(partition, low, high);
		} else {
			throw InternalException("Failed to query watermark offsets for partition " + std::to_string(partition) +
			                        ": " + RdKafka::err2str(err));
		}
	}

	auto global_state = make_uniq<TributaryScanTopicGlobalState>(partition_tasks);

	delete metadata;

	return global_state;
}

static void TributaryScanTopic(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	// All of these should be defined.
	D_ASSERT(data.local_state);
	D_ASSERT(data.global_state);
	D_ASSERT(data.bind_data);
	auto &local_state = data.local_state->Cast<TributaryScanTopicLocalState>();
	auto &global_state = data.global_state->Cast<TributaryScanTopicGlobalState>();
	auto &bind_data = data.bind_data->CastNoConst<TributaryScanTopicBindData>();

	auto &message_validity = FlatVector::Validity(output.data[3]);
	auto &topic_column = output.data[0];
	auto &message_column = output.data[3];
	auto topic_vector = FlatVector::GetData<string_t>(topic_column);
	auto partition_id_vector = FlatVector::GetData<int32_t>(output.data[1]);
	auto offset_vector = FlatVector::GetData<int64_t>(output.data[2]);
	auto message_vector = FlatVector::GetData<string_t>(message_column);

	// If we don't have a partition task to process, attempt to get one from the global state.
	// take care to properly lock the global state.

	size_t message_idx = 0;

	while (message_idx < output.GetCapacity()) {
		auto task = local_state.obtain_partition_scan_task(bind_data, global_state);

		// No task means we are done, so just return zero rows.
		if (task == std::nullopt) {
			break;
		}

		bool end_of_partition_reached = false;

		while (!end_of_partition_reached && message_idx < output.GetCapacity()) {
			std::unique_ptr<RdKafka::Message> msg(
			    local_state.consumer->consume(local_state.topic.get(), task->partition_, 1000));

			switch (msg->err()) {
			case RdKafka::ERR_NO_ERROR: {
				topic_vector[message_idx] = StringVector::AddStringOrBlob(topic_column, bind_data.topic);
				partition_id_vector[message_idx] = task->partition_;
				offset_vector[message_idx] = msg->offset();

				if (msg->payload()) {
					//					auto content = std::string(static_cast<const char *>(msg->payload()),
					// msg->len());

					message_vector[message_idx] = StringVector::AddStringOrBlob(
					    message_column, static_cast<const char *>(msg->payload()), msg->len());
				} else {
					// Need to specify that the message is null.
					message_validity.SetInvalid(message_idx);
				}

				message_idx++;

				if (msg->offset() + 1 >= task->high_) {
					end_of_partition_reached = true;
				}
				break;
			}
			case RdKafka::ERR__TIMED_OUT:
				throw InternalException("Timed out while consuming topic " + bind_data.topic + " partition " +
				                        std::to_string(task->partition_));

			case RdKafka::ERR__PARTITION_EOF:
				end_of_partition_reached = true;
				break;
			default:
				throw InternalException("Consume error on partition " + std::to_string(task->partition_) + ": " +
				                        RdKafka::err2str(msg->err()));
			}
		}

		if (end_of_partition_reached) {
			// Finished this task.
			local_state.finish_partition_scan();
		}
	}

	output.SetCardinality(message_idx);
}

// Lets work on initializing the local state
static unique_ptr<LocalTableFunctionState> TributaryScanTopicLocalInit(ExecutionContext &context,
                                                                       TableFunctionInitInput &input,
                                                                       GlobalTableFunctionState *global_state_p) {
	//    auto &info = expr.function.function_info->Cast<AirportScalarFunctionInfo>();
	// auto &data = bind_data->Cast<TributaryScanTopicBindData>();

	// The local state should contain the current partition task, if requested.
	// and the consumer objects.

	return make_uniq<TributaryScanTopicLocalState>();
}

void TributaryScanTopicAddFunction(DatabaseInstance &instance) {
	auto scan_topic_function =
	    TableFunction("tributary_scan_topic", {LogicalType::VARCHAR}, TributaryScanTopic, TributaryScanTopicBind,
	                  TributaryScanTopicGlobalInit, TributaryScanTopicLocalInit);

	for (auto &key : TributaryConfigKeys()) {
		scan_topic_function.named_parameters[key] = LogicalType(LogicalTypeId::VARCHAR);
	}

	ExtensionUtil::RegisterFunction(instance, scan_topic_function);
}

} // namespace duckdb