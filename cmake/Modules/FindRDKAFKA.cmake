# created by gbxu
# - Try to find RDKAFKA
# Once done this will define
# RDKAFKA_FOUND - System has RDKAFKA
# RDKAFKA_INCLUDE_DIRS - The RDKAFKA include directories
# RDKAFKA_LIBRARIES - The libraries needed to use RDKAFKA
# ZMQ_DEFINITIONS - Compiler switches required for using ZMQ

find_path ( RDKAFKA_INCLUDE_DIR librdkafka/rdkafka.h )
find_library ( RDKAFKA_LIBRARY NAMES rdkafka )

set ( RDKAFKA_LIBRARIES ${RDKAFKA_LIBRARY} )
set ( RDKAFKA_INCLUDE_DIRS ${RDKAFKA_INCLUDE_DIR} )

include ( FindPackageHandleStandardArgs )
# handle the QUIETLY and REQUIRED arguments and set ZMQ_FOUND to TRUE
# if all listed variables are TRUE
find_package_handle_standard_args ( RDKAFKA DEFAULT_MSG RDKAFKA_LIBRARY RDKAFKA_INCLUDE_DIR )