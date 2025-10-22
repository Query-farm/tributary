include(${VCPKG_ROOT_DIR}/scripts/cmake/vcpkg_from_github.cmake)
vcpkg_check_linkage(ONLY_STATIC_LIBRARY)

vcpkg_from_github(
    OUT_SOURCE_PATH SOURCE_PATH
    REPO query-farm/libschemaregistry
    REF b59daa3f2302386dd3833883c256b84e12e8cb39
    SHA512 668f47c458046ae0f91e6929d56e8f306e49a7a36cca69cf9a54785047cfaba05a2e5a75d568161261ecadb65de65d37c577ecb6e7bac4c459bdf199055a0a88
)


# Determine enabled features for dependency
set(ENABLED_FEATURES "")
if(DEFINED VCPKG_MANIFEST_FEATURES)
    set(ENABLED_FEATURES ${VCPKG_MANIFEST_FEATURES})
elseif(DEFINED FEATURES)
    set(ENABLED_FEATURES ${FEATURES})
endif()

# Map to dependency CMake options
foreach(f IN ITEMS avro json protobuf rules)
    string(TOUPPER ${f} F_UPPER)
    set(var_name "SCHEMAREGISTRY_WITH_${F_UPPER}")

    if("${f}" IN_LIST ENABLED_FEATURES)
        set(${var_name} ON)
    else()
        set(${var_name} OFF)
    endif()
endforeach()


vcpkg_cmake_configure(
    SOURCE_PATH ${SOURCE_PATH}
    OPTIONS
        -DSCHEMAREGISTRY_WITH_AVRO=${SCHEMAREGISTRY_WITH_AVRO}
        -DSCHEMAREGISTRY_WITH_JSON=${SCHEMAREGISTRY_WITH_JSON}
        -DSCHEMAREGISTRY_WITH_PROTOBUF=${SCHEMAREGISTRY_WITH_PROTOBUF}
        -DSCHEMAREGISTRY_WITH_RULES=${SCHEMAREGISTRY_WITH_RULES}
)

vcpkg_cmake_build()
vcpkg_cmake_install()

vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/schemaregistry)

# There can be some empty include directories
set(VCPKG_POLICY_ALLOW_EMPTY_FOLDERS enabled)

#if(EXISTS "${CURRENT_PACKAGES_DIR}/CMake")
#    vcpkg_cmake_config_fixup(CONFIG_PATH CMake)
#elseif(EXISTS "${CURRENT_PACKAGES_DIR}/lib/cmake/${PORT}")
#    vcpkg_cmake_config_fixup(CONFIG_PATH "lib/cmake/${PORT}")
#endif()


file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")

file(INSTALL "${CMAKE_CURRENT_LIST_DIR}/usage" DESTINATION "${CURRENT_PACKAGES_DIR}/share/${PORT}")

vcpkg_install_copyright(FILE_LIST "${SOURCE_PATH}/LICENSE")
