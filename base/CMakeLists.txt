include_directories(${PROJECT_SOURCE_DIR} ${GRASPER_EXTERNAL_INCLUDES})

file(GLOB base-src-files
    cpuinfo_util.cpp
    rdma.cpp
    node_util.cpp
    type.cpp
    serialization.cpp
    communication.cpp
    client_connection.cpp
    predicate.cpp
    )

add_library(base-objs OBJECT ${base-src-files})
