# created by gbxu

if (NOT __RDKAFKA_INCLUDED) # guard against multiple includes
  set(__RDKAFKA_INCLUDED TRUE)

  # use the system-wide rdkafka if present
  find_package(RDKAFKA)
  if (RDKAFKA_FOUND)
    set(RDKAFKA_EXTERNAL FALSE)
  else()
    # RDKAFKA will use pthreads if it's available in the system, so we must link with it
    find_package(Threads)

    # build directory
    set(RDKAFKA_PREFIX ${CMAKE_BINARY_DIR}/external/RDKAFKA-prefix)
    # install directory
    set(RDKAFKA_INSTALL ${CMAKE_BINARY_DIR}/external/RDKAFKA-install)

    # we build RDKAFKA statically, but want to link it into the caffe shared library
    # this requires position-independent code
    if (UNIX)
      set(RDKAFKA_EXTRA_COMPILER_FLAGS "-fPIC")
    endif()

    set(RDKAFKA_CXX_FLAGS ${CMAKE_CXX_FLAGS} ${RDKAFKA_EXTRA_COMPILER_FLAGS})
    set(RDKAFKA_C_FLAGS ${CMAKE_C_FLAGS} ${RDKAFKA_EXTRA_COMPILER_FLAGS})

    ExternalProject_Add(RDKAFKA
            PREFIX ${RDKAFKA_PREFIX}
            GIT_REPOSITORY "https://github.com/edenhill/librdkafka.git"
            UPDATE_COMMAND ""
            INSTALL_DIR ${RDKAFKA_INSTALL}
            CMAKE_ARGS -DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}
            -DCMAKE_INSTALL_PREFIX=${RDKAFKA_INSTALL}
            -DBUILD_SHARED_LIBS=OFF
            -DBUILD_STATIC_LIBS=ON
            -DBUILD_PACKAGING=OFF
            -DBUILD_TESTING=OFF
            -DBUILD_NC_TESTS=OFF
            -BUILD_CONFIG_TESTS=OFF
            -DINSTALL_HEADERS=ON
            -DCMAKE_C_FLAGS=${RDKAFKA_C_FLAGS}
            -DCMAKE_CXX_FLAGS=${RDKAFKA_CXX_FLAGS}
            LOG_DOWNLOAD 1
            LOG_INSTALL 1
            )

    set(RDKAFKA_FOUND TRUE)
    set(RDKAFKA_INCLUDE_DIRS ${RDKAFKA_INSTALL}/include/librdkafka)

    #        if(MSVC)
    #            FILE(GLOB_RECURSE KAFKA_LIBRARIES "${ZMQ_INSTALL}/lib/libzmq-${CMAKE_VS_PLATFORM_TOOLSET}*.lib")
    #set(KAFKA_LIBRARIES ${ZMQ_INSTALL}/lib/ZMQ.lib ${CMAKE_THREAD_LIBS_INIT})
    #        else()
    #            FILE(GLOB_RECURSE KAFKA_LIBRARIES "${ZMQ_INSTALL}/lib/libzmq-*.a")
    #set(KAFKA_LIBRARIES ${ZMQ_INSTALL}/lib/libZMQ.a ${CMAKE_THREAD_LIBS_INIT})
    #        endif()
    set(RDKAFKA_LIBRARY_DIRS ${RDKAFKA_INSTALL}/lib)
    set(RDKAFKA_EXTERNAL TRUE)

    list(APPEND external_project_dependencies RDKAFKA)
  endif()

endif()
