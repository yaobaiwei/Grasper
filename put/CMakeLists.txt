#Copyright 2019 Husky Data Lab, CUHK
#Authors: Hongzhi Chen


include_directories(${PROJECT_SOURCE_DIR} ${GRASPER_EXTERNAL_INCLUDES})
link_directories(${GRASPER_EXTERNAL_LIBRARIES})

add_library(global-deps STATIC
  $<TARGET_OBJECTS:iniparser-objs>
  $<TARGET_OBJECTS:utils-objs>
  $<TARGET_OBJECTS:base-objs>
  )
  
add_executable(put put.cpp)
target_link_libraries(put global-deps ${PUT_EXTERNAL_LIBRARIES})