include_directories(${PROJECT_SOURCE_DIR} ${GRASPER_EXTERNAL_INCLUDES})
 
file(GLOB store-src-files
    layout.cpp
    data_store.cpp
    ekvstore.cpp
    vkvstore.cpp
    mpi_snapshot.cpp
    )

add_library(store-objs OBJECT ${store-src-files})

#target_link_libraries(store-objs ${GRASPER_EXTERNAL_LIBRARIES})
