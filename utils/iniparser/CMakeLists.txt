include_directories(${PROJECT_SOURCE_DIR})

file(GLOB iniparser-src-files
    dictionary.c
    iniparser.c
    )

add_library(iniparser-objs OBJECT ${iniparser-src-files})
