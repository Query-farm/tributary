#include "tributary_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include <librdkafka/rdkafkacpp.h>

#include "tributary_scan_topic.hpp"
#include "tributary_config.hpp"
#include "query_farm_telemetry.hpp"

namespace duckdb {

inline void TributaryScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Tributary " + name.GetString() + " 🐥");
	});
}

struct TributaryMetadataBindData : public TableFunctionData {
	std::unordered_map<string, string> config_values_;

	std::unique_ptr<RdKafka::Conf> config;

	bool sent_row = false;

	explicit TributaryMetadataBindData(std::unordered_map<string, string> config_values)
	    : config_values_(std::move(config_values)) {

		config.reset(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));

		for (auto &kv : config_values_) {
			auto &key = kv.first;
			auto &value = kv.second;

			std::string errstr;
			printf("Setting configuration: %s = %s\n", key.c_str(), value.c_str());
			if (config->set(key, value, errstr) != RdKafka::Conf::CONF_OK) {
				throw InvalidConfigurationException("Failed to set " + key + ": " + errstr);
			}
		}
	}
};

static unique_ptr<FunctionData> TributaryMetadataBind(ClientContext &context, TableFunctionBindInput &input,
                                                      vector<LogicalType> &return_types, vector<string> &names) {

	std::unordered_map<string, string> config_values;

	for (auto &kv : input.named_parameters) {
		auto loption = StringUtil::Lower(kv.first);
		config_values[loption] = StringValue::Get(kv.second);
	}

	auto broker_info_type = LogicalType::STRUCT(
	    {{"id", LogicalTypeId::INTEGER}, {"host", LogicalTypeId::VARCHAR}, {"port", LogicalTypeId::INTEGER}});

	auto brokers_type = LogicalType::LIST(broker_info_type);

	auto partition_info_type = LogicalType::STRUCT({
	    {"id", LogicalTypeId::INTEGER},
	    {"leader", LogicalTypeId::INTEGER},
	    //{"replicas", LogicalType::LIST(LogicalType::INTEGER)},
	    //	    {"isrs", LogicalType::LIST(LogicalType::INTEGER)},
	});

	auto topics = LogicalType::LIST(LogicalType::STRUCT({{"name", LogicalTypeId::VARCHAR},
	                                                     {"error", LogicalTypeId::VARCHAR},
	                                                     {"partitions", LogicalType::LIST(partition_info_type)}

	}));

	names.push_back("brokers");
	names.push_back("topics");
	return_types.push_back(brokers_type);
	return_types.push_back(topics);

	return make_uniq<TributaryMetadataBindData>(std::move(config_values));
}

static void TributaryMetadata(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &bind_data = data.bind_data->CastNoConst<TributaryMetadataBindData>();
	std::string errstr;

	if (bind_data.sent_row) {
		output.SetCardinality(0);
		return;
	}
	bind_data.sent_row = true;

	output.SetCardinality(1);

	std::unique_ptr<RdKafka::Producer> producer(RdKafka::Producer::create(bind_data.config.get(), errstr));
	if (!producer) {
		throw InvalidConfigurationException("Failed to create Kafka producer: " + errstr);
	}

	RdKafka::Metadata *metadata_ptr = nullptr;
	RdKafka::ErrorCode err;

	// Get metadata for all topics
	err = producer->metadata(true, nullptr, &metadata_ptr, 5000);
	if (err != RdKafka::ERR_NO_ERROR) {
		throw InternalException("Failed to get metadata: " + RdKafka::err2str(err));
	}

	// Use smart pointer for automatic cleanup
	std::unique_ptr<RdKafka::Metadata> metadata(metadata_ptr);
	// Display broker information
	const RdKafka::Metadata::BrokerMetadataVector *brokers = metadata->brokers();

	auto &brokers_dest = output.data[0];

	// First resize the list.
	if (ListVector::GetListCapacity(brokers_dest) < brokers->size()) {
		ListVector::Reserve(brokers_dest, brokers->size());
	}

	{
		auto brokers_data = ListVector::GetData(brokers_dest);
		auto brokers_list_current_size = ListVector::GetListSize(brokers_dest);

		{
			auto &brokers_entries = StructVector::GetEntries(ListVector::GetEntry(brokers_dest));
			auto brokers_id_data = FlatVector::GetData<int32_t>(*brokers_entries[0]);
			auto brokers_host_data = FlatVector::GetData<string_t>(*brokers_entries[1]);
			auto brokers_port_data = FlatVector::GetData<int32_t>(*brokers_entries[2]);

			for (size_t idx = 0; idx < brokers->size(); idx++) {
				auto broker = brokers->at(idx);

				brokers_id_data[brokers_list_current_size + idx] = broker->id();
				brokers_host_data[brokers_list_current_size + idx] =
				    StringVector::AddString(*brokers_entries[1], broker->host());
				brokers_port_data[brokers_list_current_size + idx] = broker->port();
			}
		}

		brokers_data[0].length = brokers->size();
		brokers_data[0].offset = brokers_list_current_size;
		ListVector::SetListSize(brokers_dest, brokers_list_current_size + brokers->size());
	}

	{
		auto &topics_dest = output.data[1];

		auto topics_data = ListVector::GetData(topics_dest);

		const RdKafka::Metadata::TopicMetadataVector *topics = metadata->topics();

		{
			auto &topics_entries = StructVector::GetEntries(ListVector::GetEntry(topics_dest));
			auto &topic_name_column = topics_entries[0];
			auto &topic_error_column = topics_entries[1];
			auto &topic_partitions_column = topics_entries[2];

			auto topic_name_data = FlatVector::GetData<string_t>(*topic_name_column);
			auto topic_error_data = FlatVector::GetData<string_t>(*topic_error_column);

			auto &topic_error_validity = FlatVector::Validity(*topic_error_column);

			auto topic_partitions_data = ListVector::GetData(*topic_partitions_column);
			auto &topic_partitions_dest = ListVector::GetEntry(*topic_partitions_column);

			auto &topic_partition_entries = StructVector::GetEntries(topic_partitions_dest);
			auto &topic_partitions_id_column = topic_partition_entries[0];
			auto &topic_partitions_leader_column = topic_partition_entries[1];
			// auto &topic_partition_replicas_column = topic_partition_entries[2];

			auto topic_partition_name_data = FlatVector::GetData<int32_t>(*topic_partitions_id_column);
			auto topic_partition_leader_data = FlatVector::GetData<int32_t>(*topic_partitions_leader_column);

			// topic_partitions_data[0].length = 0;
			// topic_partitions_data[0].offset = 0;

			// 	// Now there is a list vector of partitions inside of each topic.

			// 	auto partitions_id_data = FlatVector::GetData<int32_t>(*partitions_id_column);
			// 	auto partitions_leader_data = FlatVector::GetData<int32_t>(*partitions_leader_column);
			const auto topics_list_current_size = ListVector::GetListSize(topics_dest);
			const auto topics_list_new_size = topics_list_current_size + topics->size();
			topics_data[0].length = topics_list_new_size;
			topics_data[0].offset = topics_list_current_size;
			ListVector::SetListSize(topics_dest, topics_list_new_size);

			for (size_t topic_idx = 0; topic_idx < topics->size(); topic_idx++) {

				auto topic = topics->at(topic_idx);

				topic_name_data[topics_list_current_size + topic_idx] =
				    StringVector::AddString(*topic_name_column, topic->topic());

				if (topic->err() != RdKafka::ERR_NO_ERROR) {
					topic_error_data[topics_list_current_size + topic_idx] =
					    StringVector::AddString(*topic_error_column, RdKafka::err2str(topic->err()));
					// 			partitions_data[0].length = 0;
					// 			partitions_data[0].offset = 0;
					continue;
				}
				topic_error_validity.SetInvalid(topic_idx);

				const RdKafka::TopicMetadata::PartitionMetadataVector *partitions = topic->partitions();

				const auto partitions_list_current_size = ListVector::GetListSize(*topic_partitions_column);
				const auto partitions_list_new_size = partitions_list_current_size + partitions->size();

				topic_partitions_data[topic_idx].length = partitions->size();
				topic_partitions_data[topic_idx].offset = partitions_list_current_size;
				ListVector::SetListSize(*topic_partitions_column, partitions_list_new_size);

				for (size_t partition_idx = 0; partition_idx < partitions->size(); partition_idx++) {
					auto partition = partitions->at(partition_idx);
					topic_partition_name_data[partitions_list_current_size + partition_idx] = partition->id();
					topic_partition_leader_data[partitions_list_current_size + partition_idx] = partition->leader();
				}
			}
		}
	}
}

static inline void TributaryVersionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	result.SetVectorType(VectorType::CONSTANT_VECTOR);
	result.SetValue(0, Value("20250612.01"));
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register a scalar function
	auto tributary_scalar_function =
	    ScalarFunction("tributary", {LogicalType::VARCHAR}, LogicalType::VARCHAR, TributaryScalarFun);
	loader.RegisterFunction(tributary_scalar_function);

	auto metadata_function = TableFunction("tributary_metadata", {}, TributaryMetadata, TributaryMetadataBind);
	for (auto &key : TributaryConfigKeys()) {
		metadata_function.named_parameters[key] = LogicalType(LogicalTypeId::VARCHAR);
	}

	loader.RegisterFunction(metadata_function);

	auto version_function = ScalarFunction("tributary_version", {}, LogicalType::VARCHAR, TributaryVersionFunction);
	loader.RegisterFunction(version_function);

	TributaryScanTopicAddFunction(loader);

	QueryFarmSendTelemetry(loader, loader.GetDatabaseInstance().shared_from_this(), "tributary", "2025092301");
}

void TributaryExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string TributaryExtension::Name() {
	return "tributary";
}

std::string TributaryExtension::Version() const {
	return "2025092301";
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(tributary, loader) {
	duckdb::LoadInternal(loader);
}
}
