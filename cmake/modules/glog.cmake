### GLOG ###

IF (GLOG_INCLUDE_DIR)
    SET (GLOG_FIND_QUIETLY TRUE)
ENDIF (GLOG_INCLUDE_DIR)

message(${GLOG_ROOT})

# find includes
FIND_PATH (GLOG_INCLUDE_DIR
    NAMES logging.h
    PATHS ${GLOG_ROOT}/include ${GLOG_ROOT}/include/glog
)

# find lib
SET(GLOG_NAME glog)

FIND_LIBRARY(GLOG_LIBRARIES
    NAMES ${GLOG_NAME}
    PATHS ${GLOG_ROOT}/lib
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("GLOG" DEFAULT_MSG GLOG_INCLUDE_DIR GLOG_LIBRARIES)

mark_as_advanced (GLOG_INCLUDE_DIR GLOG_LIBRARIES)
