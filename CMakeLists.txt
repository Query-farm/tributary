cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME tributary)

find_package(RdKafka REQUIRED)

# absl is used by schemaregistry
find_package(schemaregistry CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
src/tributary_extension.cpp
src/tributary_scan_topic.cpp
src/tributary_config.cpp
src/tributary_exception.cpp
src/tributary_logging.cpp
src/tributary_schema_registry.cpp
src/tributary_secrets.cpp
src/tributary_schema_registry_register_schema.cpp
src/tributary_schema_registry_get_schema_by_subject_and_id.cpp
src/tributary_schema_registry_get_schema_by_guid.cpp
src/tributary_schema_registry_get_schema_by_version.cpp
src/tributary_schema_registry_get_schema_latest.cpp
src/tributary_schema_registry_versions.cpp
src/tributary_schema_registry_subjects.cpp
src/tributary_schema_registry_delete_subject.cpp
src/tributary_schema_registry_delete_subject_version.cpp
src/query_farm_telemetry.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

target_link_libraries(${EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++ schemaregistry::schemaregistry)
target_link_libraries(${LOADABLE_EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++  schemaregistry::schemaregistry)

# DuckDB links every statically built extension into its own executables
# (link_extension_libraries: shell, unittest, tools/plan_serializer), so this
# target's link interface becomes a usage requirement of those C++11 DuckDB
# sources. schemaregistry brings in absl, whose INTERFACE cxx_std_17 then
# compiles tools/utils/plan_serializer.cpp as C++17: BufferedFileWriter::
# DEFAULT_OPEN_FLAGS becomes an inline (COMDAT) definition there and clashes
# with the out-of-line C++11 definition in libduckdb_static -> "multiple
# definition" at link time. Keep the link dependencies for consumers but drop
# the usage requirements (LINK_ONLY). The plain target_link_libraries signature
# is kept because build_static_extension already uses it on this target.
get_target_property(_tributary_iface ${EXTENSION_NAME} INTERFACE_LINK_LIBRARIES)
set(_tributary_iface_link_only "")
foreach(_dep IN LISTS _tributary_iface)
  if(_dep STREQUAL "duckdb_static")
    list(APPEND _tributary_iface_link_only "${_dep}")
  else()
    list(APPEND _tributary_iface_link_only "$<LINK_ONLY:${_dep}>")
  endif()
endforeach()
set_target_properties(${EXTENSION_NAME} PROPERTIES INTERFACE_LINK_LIBRARIES "${_tributary_iface_link_only}")

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
