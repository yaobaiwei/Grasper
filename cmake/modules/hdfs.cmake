### HDFS2 ###

IF (HDFS2_INCLUDE_DIR)
    SET (HDFS2_FIND_QUIETLY TRUE)
ENDIF (HDFS2_INCLUDE_DIR)

message(${HDFS2_ROOT})

# find includes
FIND_PATH (HDFS2_INCLUDE_DIR
    NAMES hdfs.h
    PATHS ${HDFS2_ROOT}/include
)

# find lib
SET(HDFS2_NAME hdfs)

FIND_LIBRARY(HDFS2_LIBRARIES
    NAMES ${HDFS2_NAME}
    PATHS ${HDFS2_ROOT}/lib ${HDFS2_ROOT}/lib/native
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("HDFS2" DEFAULT_MSG HDFS2_INCLUDE_DIR HDFS2_LIBRARIES)

mark_as_advanced (HDFS2_INCLUDE_DIR HDFS2_LIBRARIES)
