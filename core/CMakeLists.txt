include_directories(${PROJECT_SOURCE_DIR} ${GRASPER_EXTERNAL_INCLUDES})
 
file(GLOB core-src-files
	message.cpp
    rdma_mailbox.cpp
    tcp_mailbox.cpp
	parser.cpp
	expert_object.cpp
    )

add_library(core-objs OBJECT ${core-src-files})
