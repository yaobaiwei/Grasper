file(REMOVE_RECURSE
  "libglobal-deps.pdb"
  "libglobal-deps.a"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/global-deps.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
