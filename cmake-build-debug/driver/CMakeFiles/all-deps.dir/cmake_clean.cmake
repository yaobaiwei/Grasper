file(REMOVE_RECURSE
  "liball-deps.pdb"
  "liball-deps.a"
)

# Per-language clean rules from dependency scanning.
foreach(lang C CXX)
  include(CMakeFiles/all-deps.dir/cmake_clean_${lang}.cmake OPTIONAL)
endforeach()
