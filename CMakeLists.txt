cmake_minimum_required(VERSION 2.8.7)

project(pskafka C CXX)

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
endif()

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake/Modules)

include(ExternalProject) 
set(pskafka_LINKER_LIBS_L "" )
set(pskafka_LINKER_LIBS_L_RELEASE "" )
set(pskafka_LINKER_LIBS_L_DEBUG "" )
set(pskafka_INCLUDE_DIR_L "" )

# ---[ rdkafka
include("cmake/External/rdkafka.cmake")
include_directories(pskafka ${RDKAFKA_INCLUDE_DIRS})
list(APPEND pskafka_LINKER_LIBS_L ${RDKAFKA_LIBRARIES})
# ---[ Google-protobuf
include(cmake/ProtoBuf.cmake)

# generate protobuf sources
set(proto_gen_folder "${PROJECT_BINARY_DIR}/src")
file(GLOB_RECURSE proto_files "src/*.proto")
pskafka_protobuf_generate_cpp_py(${proto_gen_folder} proto_srcs proto_hdrs proto_python "${PROJECT_SOURCE_DIR}" "src" ${proto_files})
include_directories(pskafka "${PROJECT_SOURCE_DIR}/include/")
include_directories(pskafka "${PROJECT_BINARY_DIR}/include/")
include_directories(pskafka "${PROJECT_BINARY_DIR}/src/")
list(APPEND pskafka_INCLUDE_DIR_L "${PROJECT_BINARY_DIR}/include/")

#FILE(COPY DIRECTORY "${PROJECT_BINARY_DIR}/include" DESTINATION "${PROJECT_SOURCE_DIR}/" FILES_MATCHING PATTERN "*.pb.h")
FILE(GLOB SOURCE "src/*.cc")

if(MSVC)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /EHsc")
FILE(GLOB getopt_SOURCE "src/windows/getopt.c")
list(APPEND SOURCE ${getopt_SOURCE}) 
add_definitions(-DSTATIC_GETOPT)
include_directories(pskafka "${PROJECT_SOURCE_DIR}/src/windows")
list(APPEND pskafka_LINKER_LIBS_L "ipHlpApi.lib" "ws2_32.lib")
  foreach(flag_var
        CMAKE_CXX_FLAGS CMAKE_CXX_FLAGS_DEBUG CMAKE_CXX_FLAGS_RELEASE
        CMAKE_CXX_FLAGS_MINSIZEREL CMAKE_CXX_FLAGS_RELWITHDEBINFO)
    if(${flag_var} MATCHES "/MD")
      string(REGEX REPLACE "/MD" "/MT" ${flag_var} "${${flag_var}}")
    endif(${flag_var} MATCHES "/MD")
  endforeach(flag_var)
endif()

list(APPEND SOURCE ${proto_srcs}) 
add_library(pskafka ${SOURCE})

target_link_libraries(pskafka ${pskafka_LINKER_LIBS})

# FindProtobuf behavior changed from cmake 3.6 onwards
IF (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} GREATER 3.6)
  set(PROTO_LIB ${Protobuf_LIBRARY})
  set(PROTO_LIB_DEBUG ${Protobuf_LIBRARY_DEBUG})
ELSE (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} GREATER 3.6)
  set(PROTO_LIB ${PROTOBUF_LIBRARY})
  set(PROTO_LIB_DEBUG ${PROTOBUF_LIBRARY_DEBUG})
ENDIF (${CMAKE_MAJOR_VERSION}.${CMAKE_MINOR_VERSION} GREATER 3.6)

list(APPEND pskafka_LINKER_LIBS_L_RELEASE ${PROTO_LIB})
list(APPEND pskafka_LINKER_LIBS_L_DEBUG ${PROTO_LIB_DEBUG})

list(APPEND pskafka_INCLUDE_DIR_L "${PROJECT_SOURCE_DIR}/include")
list(APPEND pskafka_INCLUDE_DIR_L ${PROTOBUF_INCLUDE_DIR})

set(pskafka_LINKER_LIBS ${pskafka_LINKER_LIBS_L} PARENT_SCOPE)
set(pskafka_LINKER_LIBS_RELEASE ${pskafka_LINKER_LIBS_L_RELEASE} PARENT_SCOPE)
set(pskafka_LINKER_LIBS_DEBUG ${pskafka_LINKER_LIBS_L_DEBUG} PARENT_SCOPE)
set(pskafka_INCLUDE_DIR ${pskafka_INCLUDE_DIR_L} PARENT_SCOPE)
