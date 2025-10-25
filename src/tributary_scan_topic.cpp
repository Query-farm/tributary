#include "tributary_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <librdkafka/rdkafkacpp.h>
#include "tributary_config.hpp"
#include <optional>
#include "tributary_logging.hpp"
#include "tributary_exception.hpp"

// JSON library
#include <nlohmann/json.hpp>

// Schema Registry Client includes
#include "schemaregistry/rest/ClientConfiguration.h"
#include "schemaregistry/rest/SchemaRegistryClient.h"
#include "schemaregistry/rest/model/Schema.h"
#include "schemaregistry/serdes/SerdeConfig.h"
#include "schemaregistry/serdes/SerdeTypes.h"
#include "schemaregistry/serdes/json/JsonSerializer.h"
#include "schemaregistry/serdes/json/JsonDeserializer.h"

#include "tributary_schema_registry.hpp"

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
	std::unique_ptr<TributaryEventCb> event_cb;
	const std::string topic;

	string schema_registry_url;

	std::optional<schemaregistry::rest::model::RegisteredSchema> key_schema_result;
	std::optional<schemaregistry::rest::model::RegisteredSchema> value_schema_result;
	std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> schema_registry_client;

	explicit TributaryScanTopicBindData(
	    ClientContext &context, std::string topic, std::string schema_registry_url,
	    std::unordered_map<string, string> config_values,
	    std::optional<schemaregistry::rest::model::RegisteredSchema> key_schema_result,
	    std::optional<schemaregistry::rest::model::RegisteredSchema> message_schema_result,
	    std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> schema_registry_client)
	    : topic(std::move(topic)), schema_registry_url(std::move(schema_registry_url)),
	      key_schema_result(std::move(key_schema_result)), value_schema_result(std::move(message_schema_result)),
	      schema_registry_client(std::move(schema_registry_client)) {

		config.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

		std::string errstr;
		for (auto &kv : config_values) {
			auto &key = kv.first;
			auto &value = kv.second;

			if (config->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
				throw TributaryException(ExceptionType::INVALID_CONFIGURATION, errstr, this->topic);
			}
		}

		//		if (config->set("auto.offset.reset", "earliest", errstr) != RdKafka::Conf::CONF_OK) {
		//			throw TributaryException(ExceptionType::INVALID_CONFIGURATION, errstr, this->topic);
		//		}

		if (config->set("enable.partition.eof", "true", errstr) != RdKafka::Conf::CONF_OK) {
			throw TributaryException(ExceptionType::INVALID_CONFIGURATION, errstr, this->topic);
		}

		event_cb = std::make_unique<TributaryEventCb>(context.shared_from_this());

		if (config->set("event_cb", event_cb.get(), errstr) != RdKafka::Conf::CONF_OK) {
			throw TributaryException(ExceptionType::INVALID_CONFIGURATION, errstr, this->topic);
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

// struct TributaryScanTopicLocalState2 : public LocalTableFunctionState {

// 	std::optional<TributaryScanTopicPartitionTask> current_task_ = std::nullopt;
// 	std::unique_ptr<RdKafka::Consumer> consumer = nullptr;
// 	std::unique_ptr<RdKafka::Topic> topic = nullptr;

// 	std::unique_ptr<schemaregistry::serdes::SerializationContext> key_serialization_context = nullptr;
// 	std::unique_ptr<schemaregistry::serdes::SerializationContext> value_serialization_context = nullptr;
// 	std::unique_ptr<schemaregistry::serdes::json::JsonDeserializer> json_deserializer = nullptr;

// 	explicit TributaryScanTopicLocalState(
// 	    std::unique_ptr<schemaregistry::serdes::SerializationContext> key_serialization_context,
// 	    std::unique_ptr<schemaregistry::serdes::SerializationContext> value_serialization_context,
// 	    std::unique_ptr<schemaregistry::serdes::json::JsonDeserializer> json_deserializer_ptr)
// 	    : LocalTableFunctionState(), key_serialization_context(std::move(key_serialization_context)),
// 	      value_serialization_context(std::move(value_serialization_context)),
// 	      json_deserializer(std::move(json_deserializer_ptr)) {
// 	}

// 	std::optional<TributaryScanTopicPartitionTask>
// 	obtain_partition_scan_task(TributaryScanTopicBindData &bind_data, TributaryScanTopicGlobalState &global_state) {
// 		if (current_task_.has_value()) {
// 			return current_task_;
// 		}

// 		auto task = global_state.obtain_task();
// 		if (!task.has_value()) {
// 			// No more tasks available
// 			return std::nullopt;
// 		}

// 		std::string errstr;
// 		consumer = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(bind_data.config.get(), errstr));
// 		if (!consumer) {
// 			throw TributaryException(ExceptionType::NETWORK, "Failed to create Kafka consumer: " + errstr);
// 		}

// 		std::unique_ptr<RdKafka::Conf> tconf(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

// 		topic = std::unique_ptr<RdKafka::Topic>(
// 		    RdKafka::Topic::create(consumer.get(), bind_data.topic, tconf.get(), errstr));

// 		if (!topic) {
// 			throw TributaryException(ExceptionType::NETWORK, "Failed to create Kafka topic object: " + errstr);
// 		}

// 		RdKafka::ErrorCode err = consumer->start(topic.get(), task->partition_, RdKafka::Topic::OFFSET_BEGINNING);
// 		if (err != RdKafka::ERR_NO_ERROR) {
// 			throw TributaryException(ExceptionType::NETWORK, "Failed to start consumer", err, topic->name(),
// 			                         task->partition_);
// 		}
// 		current_task_ = task;
// 		return current_task_;
// 	}

// 	void finish_partition_scan() {
// 		if (current_task_.has_value()) {
// 			// We are done with this task, so we can reset it.
// 			auto err = consumer->stop(topic.get(), current_task_->partition_);
// 			if (err != RdKafka::ERR_NO_ERROR) {
// 				throw TributaryException(ExceptionType::NETWORK, "Failed to stop consumer", err, topic->name(),
// 				                         current_task_->partition_);
// 			}

// 			current_task_ = std::nullopt;
// 		}
// 	}
// };

struct TributaryScanTopicLocalState : public LocalTableFunctionState {

	std::optional<TributaryScanTopicPartitionTask> current_task_ = std::nullopt;

	// Consumer and topic live for the lifetime of this object
	std::unique_ptr<RdKafka::Consumer> consumer = nullptr;
	std::unique_ptr<RdKafka::Topic> topic = nullptr;

	std::unique_ptr<schemaregistry::serdes::SerializationContext> key_serialization_context = nullptr;
	std::unique_ptr<schemaregistry::serdes::SerializationContext> value_serialization_context = nullptr;
	std::unique_ptr<schemaregistry::serdes::json::JsonDeserializer> json_deserializer = nullptr;

	explicit TributaryScanTopicLocalState(
	    std::unique_ptr<schemaregistry::serdes::SerializationContext> key_serialization_context,
	    std::unique_ptr<schemaregistry::serdes::SerializationContext> value_serialization_context,
	    std::unique_ptr<schemaregistry::serdes::json::JsonDeserializer> json_deserializer_ptr)
	    : LocalTableFunctionState(), key_serialization_context(std::move(key_serialization_context)),
	      value_serialization_context(std::move(value_serialization_context)),
	      json_deserializer(std::move(json_deserializer_ptr)) {
	}

	// Initialize the consumer and topic once
	void initialize_consumer_topic(TributaryScanTopicBindData &bind_data) {
		if (consumer && topic)
			return; // already initialized

		std::string errstr;
		consumer = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(bind_data.config.get(), errstr));
		if (!consumer) {
			throw TributaryException(ExceptionType::NETWORK, "Failed to create Kafka consumer: " + errstr);
		}

		auto tconf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));
		topic = std::unique_ptr<RdKafka::Topic>(
		    RdKafka::Topic::create(consumer.get(), bind_data.topic, tconf.get(), errstr));
		if (!topic) {
			throw TributaryException(ExceptionType::NETWORK, "Failed to create Kafka topic object: " + errstr);
		}
	}

	std::optional<TributaryScanTopicPartitionTask>
	obtain_partition_scan_task(TributaryScanTopicBindData &bind_data, TributaryScanTopicGlobalState &global_state) {
		if (current_task_.has_value())
			return current_task_;

		auto task = global_state.obtain_task();
		if (!task.has_value())
			return std::nullopt;

		// Make sure consumer and topic exist
		initialize_consumer_topic(bind_data);

		// Start the partition for this task
		RdKafka::ErrorCode err = consumer->start(topic.get(), task->partition_, RdKafka::Topic::OFFSET_BEGINNING);
		if (err != RdKafka::ERR_NO_ERROR) {
			throw TributaryException(ExceptionType::NETWORK, "Failed to start consumer", err, topic->name(),
			                         task->partition_);
		}

		current_task_ = task;
		return current_task_;
	}

	void finish_partition_scan() {
		if (!current_task_.has_value())
			return;

		// Stop the partition
		auto err = consumer->stop(topic.get(), current_task_->partition_);
		if (err != RdKafka::ERR_NO_ERROR) {
			throw TributaryException(ExceptionType::NETWORK, "Failed to stop consumer", err, topic->name(),
			                         current_task_->partition_);
		}

		current_task_ = std::nullopt;
	}

	~TributaryScanTopicLocalState() override {
		// Ensure all partitions are stopped before destruction
		if (consumer && topic) {
			consumer->stop(topic.get(), RdKafka::Topic::OFFSET_BEGINNING); // best effort
		}
	}
};

static vector<std::pair<string, LogicalType>>
TributarySchemaRegistryConvertSchemaToFields(const std::string &topic, const std::string &subject, bool is_key,
                                             schemaregistry::rest::model::RegisteredSchema &schema) {

	vector<std::pair<string, LogicalType>> result_fields;

	if (!schema.getSchemaType().has_value()) {
		throw TributaryException(ExceptionType::INVALID_TYPE,
		                         "Schema type is not specified in the schema registry for subject: " + subject, topic);
	}
	auto schema_type = schema.getSchemaType().value();
	if (schema_type != "JSON") {
		throw TributaryException(
		    ExceptionType::INVALID_TYPE,
		    "Unsupported schema type '" + schema_type + "' in the schema registry for subject: " + subject, topic);
	}

	// For JSON we will just use the JSON fields.
	result_fields.push_back(std::make_pair(is_key ? "key" : "value", LogicalType::JSON()));
	return result_fields;
}

static unique_ptr<FunctionData> TributaryScanTopicBind(ClientContext &context, TableFunctionBindInput &input,
                                                       vector<LogicalType> &return_types, vector<string> &names) {

	auto topic = input.inputs[0].GetValue<string>();

	std::unordered_map<string, string> config_values;
	string schema_registry_url;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);

		if (loption == "schema.registry.url") {
			schema_registry_url = StringValue::Get(kv.second);
		} else {
			config_values[loption] = StringValue::Get(kv.second);
		}
	}

	vector<std::pair<string, LogicalType>> result_fields = {
	    {"topic", LogicalType(LogicalTypeId::VARCHAR)},
	    {"partition", LogicalType(LogicalTypeId::INTEGER)},
	    {"offset", LogicalType(LogicalTypeId::BIGINT)},
	};

	LogicalType key_type = LogicalType(LogicalTypeId::BLOB);
	LogicalType message_type = LogicalType(LogicalTypeId::BLOB);
	std::optional<schemaregistry::rest::model::RegisteredSchema> key_schema_result = std::nullopt;
	std::optional<schemaregistry::rest::model::RegisteredSchema> message_schema_result = std::nullopt;

	std::shared_ptr<schemaregistry::rest::ISchemaRegistryClient> schema_registry_client = nullptr;

	if (schema_registry_url.empty()) {
		result_fields.push_back(std::make_pair("key", LogicalType(LogicalTypeId::BLOB)));
		result_fields.push_back(std::make_pair("message", LogicalType(LogicalTypeId::BLOB)));
	} else {
		// If we have a schema registry specified, lets lookup the schema for the key and message
		auto key_subject_name = topic + "-key";
		auto message_subject_name = topic + "-value";

		auto client_config =
		    std::make_shared<schemaregistry::rest::ClientConfiguration>(std::vector<std::string> {schema_registry_url});

		// Retrieve secret for authentication if it exists.
		TributarySchemaRegistryPopulateAuth(*client_config, context, schema_registry_url);

		// Create Schema Registry client
		schema_registry_client = schemaregistry::rest::SchemaRegistryClient::newClient(client_config);

		try {
			auto key_result = schema_registry_client->getLatestVersion(key_subject_name);
			key_schema_result = std::make_optional<schemaregistry::rest::model::RegisteredSchema>(key_result);
			for (auto &p : TributarySchemaRegistryConvertSchemaToFields(topic, key_subject_name, true, key_result)) {
				result_fields.push_back(p);
			}
		} catch (const schemaregistry::rest::RestException &ex) {
			if (ex.getStatus() == 404) {
				// There may not be a key schema, so we can just use BLOB for the key.
				result_fields.push_back(std::make_pair("key", LogicalType(LogicalTypeId::BLOB)));
			} else {
				// just rethrow
				throw;
			}
		}

		try {
			auto message_result = schema_registry_client->getLatestVersion(message_subject_name);
			message_schema_result = std::make_optional<schemaregistry::rest::model::RegisteredSchema>(message_result);
			for (auto &p :
			     TributarySchemaRegistryConvertSchemaToFields(topic, message_subject_name, false, message_result)) {
				result_fields.push_back(p);
			}
		} catch (const schemaregistry::rest::RestException &ex) {
			if (ex.getStatus() == 404) {
				// There may not be a key schema, so we can just use BLOB for the key.
				result_fields.push_back(std::make_pair("message", LogicalType(LogicalTypeId::BLOB)));
			} else {
				// just rethrow
				throw;
			}
		}
	}

	for (auto &field : result_fields) {
		names.push_back(field.first);
		return_types.push_back(field.second);
	}

	return make_uniq<TributaryScanTopicBindData>(context, std::move(topic), std::move(schema_registry_url),
	                                             std::move(config_values), key_schema_result, message_schema_result,
	                                             schema_registry_client);
}

static unique_ptr<GlobalTableFunctionState> TributaryScanTopicGlobalInit(ClientContext &context,
                                                                         TableFunctionInitInput &input) {

	auto &bind_data = input.bind_data->Cast<TributaryScanTopicBindData>();

	std::string errstr;
	auto consumer = std::unique_ptr<RdKafka::Consumer>(RdKafka::Consumer::create(bind_data.config.get(), errstr));

	if (!consumer) {
		throw TributaryException(ExceptionType::NETWORK, "Failed to create Kafka consumer", errstr);
	}

	// FIXME: allow additional config to be specified here.
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	auto topic =
	    std::unique_ptr<RdKafka::Topic>(RdKafka::Topic::create(consumer.get(), bind_data.topic, tconf, errstr));

	if (!topic) {
		throw TributaryException(ExceptionType::NETWORK, errstr, bind_data.topic);
	}

	RdKafka::Metadata *metadata;
	RdKafka::ErrorCode err = consumer->metadata(false, topic.get(), &metadata, 5000);
	if (err != RdKafka::ERR_NO_ERROR) {
		throw TributaryException(ExceptionType::NETWORK, "Failed to get topic metadata", err, bind_data.topic);
	}

	bool exists = false;
	const RdKafka::TopicMetadata *topic_metadata = nullptr;
	if (metadata->topics()->size() == 1) {
		topic_metadata = metadata->topics()->at(0);
		exists = (topic_metadata->err() != RdKafka::ERR_UNKNOWN_TOPIC_OR_PART);
	}

	if (!exists) {
		delete metadata;
		throw TributaryException(ExceptionType::NETWORK, "Not found", bind_data.topic);
	}

	std::vector<TributaryScanTopicPartitionTask> partition_tasks;
	// Get partitions and their watermarks
	auto partitions = topic_metadata->partitions();

	for (auto it = partitions->begin(); it != partitions->end(); ++it) {
		int32_t partition = (*it)->id();
		int64_t low, high;

		err = consumer->query_watermark_offsets(bind_data.topic, partition, &low, &high, 5000);
		if (err == RdKafka::ERR_NO_ERROR) {
			if (low != high) {
				// don't scan empty partitions.
				partition_tasks.emplace_back(partition, low, high);
			}
		} else {
			throw TributaryException(ExceptionType::NETWORK, "Failed to query watermark offsets for partition ", err,
			                         bind_data.topic, partition);
		}
	}

	DUCKDB_LOG(context, TributaryLogType, "Initialized Tributary scan",
	           {{"partitions", to_string(partition_tasks.size())}, {"topic", bind_data.topic}});
	for (auto &task : partition_tasks) {
		DUCKDB_LOG(context, TributaryLogType, "Partition watermarks",
		           {{"partition", to_string(task.partition_)},
		            {"low", to_string(task.low_)},
		            {"high", to_string(task.high_)}});
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

	auto &key_validity = FlatVector::Validity(output.data[3]);
	auto &message_validity = FlatVector::Validity(output.data[4]);
	auto &topic_column = output.data[0];
	auto &key_column = output.data[3];
	auto &message_column = output.data[4];
	auto topic_vector = FlatVector::GetData<string_t>(topic_column);
	auto partition_id_vector = FlatVector::GetData<int32_t>(output.data[1]);
	auto offset_vector = FlatVector::GetData<int64_t>(output.data[2]);
	auto key_vector = FlatVector::GetData<string_t>(key_column);
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
			    local_state.consumer->consume(local_state.topic.get(), task->partition_, 2000));

			switch (msg->err()) {
			case RdKafka::ERR_NO_ERROR: {
				topic_vector[message_idx] = StringVector::AddStringOrBlob(topic_column, bind_data.topic);
				partition_id_vector[message_idx] = task->partition_;
				offset_vector[message_idx] = msg->offset();

				if (msg->key()) {

					if (local_state.key_serialization_context != nullptr) {
						// In C++20 this doesn't have to be a copy.
						std::vector<uint8_t> payload(reinterpret_cast<const uint8_t *>(msg->key()->data()),
						                             reinterpret_cast<const uint8_t *>(msg->key()->data()) +
						                                 msg->key()->size());

						auto deserialized_key =
						    local_state.json_deserializer->deserialize(*local_state.key_serialization_context, payload);

						key_vector[message_idx] = StringVector::AddStringOrBlob(key_column, deserialized_key.dump());
					} else {
						key_vector[message_idx] = StringVector::AddStringOrBlob(
						    key_column, static_cast<const char *>(msg->key()->c_str()), msg->key()->size());
					}
				} else {
					// Need to specify that the key is null.
					key_validity.SetInvalid(message_idx);
				}

				if (msg->payload()) {
					if (local_state.value_serialization_context != nullptr) {
						// In C++20 this doesn't have to be a copy.
						std::vector<uint8_t> payload(reinterpret_cast<const uint8_t *>(msg->payload()),
						                             reinterpret_cast<const uint8_t *>(msg->payload()) + msg->len());

						auto deserialized_message = local_state.json_deserializer->deserialize(
						    *local_state.value_serialization_context, payload);

						message_vector[message_idx] =
						    StringVector::AddStringOrBlob(message_column, deserialized_message.dump());
					} else {
						message_vector[message_idx] = StringVector::AddStringOrBlob(
						    message_column, static_cast<const char *>(msg->payload()), msg->len());
					}
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
				throw TributaryException(ExceptionType::NETWORK, msg->err(), bind_data.topic, task->partition_);

			case RdKafka::ERR__PARTITION_EOF:
				end_of_partition_reached = true;
				break;
			default:
				throw TributaryException(ExceptionType::NETWORK, msg->err(), bind_data.topic, task->partition_);
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
	auto &bind_data = input.bind_data->Cast<TributaryScanTopicBindData>();

	// The local state should contain the current partition task, if requested.
	// and the consumer objects.

	// Create deserializer configuration

	std::unique_ptr<schemaregistry::serdes::SerializationContext> key_serialization_context = nullptr;
	std::unique_ptr<schemaregistry::serdes::SerializationContext> value_serialization_context = nullptr;

	if (bind_data.key_schema_result.has_value()) {
		if (bind_data.key_schema_result->getSchemaType().value() != "JSON") {
			throw TributaryException(ExceptionType::INVALID_TYPE,
			                         "Unsupported schema type '" +
			                             bind_data.key_schema_result->getSchemaType().value() +
			                             "' for key deserializer",
			                         bind_data.topic);
		}

		key_serialization_context = std::make_unique<schemaregistry::serdes::SerializationContext>();
		key_serialization_context->topic = bind_data.topic;
		key_serialization_context->serde_type = schemaregistry::serdes::SerdeType::Key;
		key_serialization_context->serde_format = schemaregistry::serdes::SerdeFormat::Json;
		key_serialization_context->headers = std::nullopt;
	}

	if (bind_data.value_schema_result.has_value()) {
		if (bind_data.value_schema_result->getSchemaType().value() != "JSON") {
			throw TributaryException(ExceptionType::INVALID_TYPE,
			                         "Unsupported schema type '" +
			                             bind_data.value_schema_result->getSchemaType().value() +
			                             "' for message deserializer",
			                         bind_data.topic);
		}
		value_serialization_context = std::make_unique<schemaregistry::serdes::SerializationContext>();
		value_serialization_context->topic = bind_data.topic;
		value_serialization_context->serde_type = schemaregistry::serdes::SerdeType::Value;
		value_serialization_context->serde_format = schemaregistry::serdes::SerdeFormat::Json;
		value_serialization_context->headers = std::nullopt;
	}

	std::unique_ptr<schemaregistry::serdes::json::JsonDeserializer> json_deserializer = nullptr;

	std::unordered_map<std::string, std::string> rule_config;
	schemaregistry::serdes::DeserializerConfig deser_config(std::nullopt, false, rule_config);

	if ((key_serialization_context != nullptr &&
	     key_serialization_context->serde_format == schemaregistry::serdes::SerdeFormat::Json) ||
	    (value_serialization_context != nullptr &&
	     value_serialization_context->serde_format == schemaregistry::serdes::SerdeFormat::Json)) {

		json_deserializer = std::make_unique<schemaregistry::serdes::json::JsonDeserializer>(
		    bind_data.schema_registry_client, nullptr, deser_config);
	}

	return make_uniq<TributaryScanTopicLocalState>(
	    std::move(key_serialization_context), std::move(value_serialization_context), std::move(json_deserializer));
}

void TributaryScanTopicAddFunction(ExtensionLoader &loader) {

	auto scan_topic_function = TableFunctionSet("tributary_scan_topic");

	auto no_options = TableFunction("tributary_scan_topic", {LogicalType::VARCHAR}, TributaryScanTopic,
	                                TributaryScanTopicBind, TributaryScanTopicGlobalInit, TributaryScanTopicLocalInit);

	for (auto &key : TributaryConfigKeys()) {
		no_options.named_parameters[key] = LogicalType(LogicalTypeId::VARCHAR);
	}

	// So we should support a limit option for total records to scan.
	// no_options.named_parameters["limit"] = LogicalType(LogicalTypeId::UBIGINT);

	scan_topic_function.AddFunction(no_options);

	loader.RegisterFunction(scan_topic_function);
}

} // namespace duckdb