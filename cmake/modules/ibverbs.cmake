### IBVERB ###

IF (IBVERB_INCLUDE_DIR)
    SET (IBVERB_FIND_QUIETLY TRUE)
ENDIF (IBVERB_INCLUDE_DIR)

message(${IBVERB_ROOT})

# find includes
FIND_PATH (IBVERB_INCLUDE_DIR
    NAMES verbs.h
    PATHS ${IBVERB_ROOT}/include ${IBVERB_ROOT}/include/infiniband
)

# find lib
SET(IBVERB_NAME ibverbs)

FIND_LIBRARY(IBVERB_LIBRARIES
    NAMES ${IBVERB_NAME}
    PATHS ${IBVERB_ROOT}/lib
    NO_DEFAULT_PATH
)

include ("FindPackageHandleStandardArgs")
find_package_handle_standard_args ("IBVERB" DEFAULT_MSG IBVERB_INCLUDE_DIR IBVERB_LIBRARIES)

mark_as_advanced (IBVERB_INCLUDE_DIR IBVERB_LIBRARIES)
