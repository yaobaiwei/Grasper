include_directories(${PROJECT_SOURCE_DIR} ${GRASPER_EXTERNAL_INCLUDES})

file(GLOB expert-src-files
        abstract_expert.hpp
        as_expert.hpp
        barrier_expert.hpp
        branch_expert.hpp
        config_expert.hpp
        expert_cache.hpp
        expert_lru_cache.hpp
        has_expert.hpp
        has_label_expert.hpp
        index_expert.hpp
        init_expert.hpp
        is_expert.hpp
        key_expert.hpp
        label_expert.hpp
        labelled_branch_expert.hpp
        properties_expert.hpp
        repeat_expert.hpp
        select_expert.hpp
        traversal_expert.hpp
        values_expert.hpp
        where_expert.hpp
        until_expert.hpp
        )

add_library(expert-objs OBJECT ${expert-src-files})
