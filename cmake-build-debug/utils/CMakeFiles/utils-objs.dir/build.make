# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 3.12

# Delete rule output on recipe failure.
.DELETE_ON_ERROR:


#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:


# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list


# Suppress display of executed commands.
$(VERBOSE).SILENT:


# A target that is always out of date.
cmake_force:

.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake

# The command to remove a file.
RM = /Applications/CLion.app/Contents/bin/cmake/mac/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /Users/oruqimaru/Desktop/FYP/Grasper

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug

# Include any dependencies generated for this target.
include utils/CMakeFiles/utils-objs.dir/depend.make

# Include the progress variables for this target.
include utils/CMakeFiles/utils-objs.dir/progress.make

# Include the compile flags for this target's objects.
include utils/CMakeFiles/utils-objs.dir/flags.make

utils/CMakeFiles/utils-objs.dir/console_util.cpp.o: utils/CMakeFiles/utils-objs.dir/flags.make
utils/CMakeFiles/utils-objs.dir/console_util.cpp.o: ../utils/console_util.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_1) "Building CXX object utils/CMakeFiles/utils-objs.dir/console_util.cpp.o"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/utils-objs.dir/console_util.cpp.o -c /Users/oruqimaru/Desktop/FYP/Grasper/utils/console_util.cpp

utils/CMakeFiles/utils-objs.dir/console_util.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utils-objs.dir/console_util.cpp.i"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/oruqimaru/Desktop/FYP/Grasper/utils/console_util.cpp > CMakeFiles/utils-objs.dir/console_util.cpp.i

utils/CMakeFiles/utils-objs.dir/console_util.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utils-objs.dir/console_util.cpp.s"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/oruqimaru/Desktop/FYP/Grasper/utils/console_util.cpp -o CMakeFiles/utils-objs.dir/console_util.cpp.s

utils/CMakeFiles/utils-objs.dir/global.cpp.o: utils/CMakeFiles/utils-objs.dir/flags.make
utils/CMakeFiles/utils-objs.dir/global.cpp.o: ../utils/global.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_2) "Building CXX object utils/CMakeFiles/utils-objs.dir/global.cpp.o"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/utils-objs.dir/global.cpp.o -c /Users/oruqimaru/Desktop/FYP/Grasper/utils/global.cpp

utils/CMakeFiles/utils-objs.dir/global.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utils-objs.dir/global.cpp.i"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/oruqimaru/Desktop/FYP/Grasper/utils/global.cpp > CMakeFiles/utils-objs.dir/global.cpp.i

utils/CMakeFiles/utils-objs.dir/global.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utils-objs.dir/global.cpp.s"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/oruqimaru/Desktop/FYP/Grasper/utils/global.cpp -o CMakeFiles/utils-objs.dir/global.cpp.s

utils/CMakeFiles/utils-objs.dir/hdfs_core.cpp.o: utils/CMakeFiles/utils-objs.dir/flags.make
utils/CMakeFiles/utils-objs.dir/hdfs_core.cpp.o: ../utils/hdfs_core.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_3) "Building CXX object utils/CMakeFiles/utils-objs.dir/hdfs_core.cpp.o"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/utils-objs.dir/hdfs_core.cpp.o -c /Users/oruqimaru/Desktop/FYP/Grasper/utils/hdfs_core.cpp

utils/CMakeFiles/utils-objs.dir/hdfs_core.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utils-objs.dir/hdfs_core.cpp.i"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/oruqimaru/Desktop/FYP/Grasper/utils/hdfs_core.cpp > CMakeFiles/utils-objs.dir/hdfs_core.cpp.i

utils/CMakeFiles/utils-objs.dir/hdfs_core.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utils-objs.dir/hdfs_core.cpp.s"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/oruqimaru/Desktop/FYP/Grasper/utils/hdfs_core.cpp -o CMakeFiles/utils-objs.dir/hdfs_core.cpp.s

utils/CMakeFiles/utils-objs.dir/mkl_util.cpp.o: utils/CMakeFiles/utils-objs.dir/flags.make
utils/CMakeFiles/utils-objs.dir/mkl_util.cpp.o: ../utils/mkl_util.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_4) "Building CXX object utils/CMakeFiles/utils-objs.dir/mkl_util.cpp.o"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/utils-objs.dir/mkl_util.cpp.o -c /Users/oruqimaru/Desktop/FYP/Grasper/utils/mkl_util.cpp

utils/CMakeFiles/utils-objs.dir/mkl_util.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utils-objs.dir/mkl_util.cpp.i"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/oruqimaru/Desktop/FYP/Grasper/utils/mkl_util.cpp > CMakeFiles/utils-objs.dir/mkl_util.cpp.i

utils/CMakeFiles/utils-objs.dir/mkl_util.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utils-objs.dir/mkl_util.cpp.s"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/oruqimaru/Desktop/FYP/Grasper/utils/mkl_util.cpp -o CMakeFiles/utils-objs.dir/mkl_util.cpp.s

utils/CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.o: utils/CMakeFiles/utils-objs.dir/flags.make
utils/CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.o: ../utils/mpi_unique_namer.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_5) "Building CXX object utils/CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.o"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.o -c /Users/oruqimaru/Desktop/FYP/Grasper/utils/mpi_unique_namer.cpp

utils/CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.i"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/oruqimaru/Desktop/FYP/Grasper/utils/mpi_unique_namer.cpp > CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.i

utils/CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.s"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/oruqimaru/Desktop/FYP/Grasper/utils/mpi_unique_namer.cpp -o CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.s

utils/CMakeFiles/utils-objs.dir/tid_mapper.cpp.o: utils/CMakeFiles/utils-objs.dir/flags.make
utils/CMakeFiles/utils-objs.dir/tid_mapper.cpp.o: ../utils/tid_mapper.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_6) "Building CXX object utils/CMakeFiles/utils-objs.dir/tid_mapper.cpp.o"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/utils-objs.dir/tid_mapper.cpp.o -c /Users/oruqimaru/Desktop/FYP/Grasper/utils/tid_mapper.cpp

utils/CMakeFiles/utils-objs.dir/tid_mapper.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utils-objs.dir/tid_mapper.cpp.i"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/oruqimaru/Desktop/FYP/Grasper/utils/tid_mapper.cpp > CMakeFiles/utils-objs.dir/tid_mapper.cpp.i

utils/CMakeFiles/utils-objs.dir/tid_mapper.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utils-objs.dir/tid_mapper.cpp.s"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/oruqimaru/Desktop/FYP/Grasper/utils/tid_mapper.cpp -o CMakeFiles/utils-objs.dir/tid_mapper.cpp.s

utils/CMakeFiles/utils-objs.dir/timer.cpp.o: utils/CMakeFiles/utils-objs.dir/flags.make
utils/CMakeFiles/utils-objs.dir/timer.cpp.o: ../utils/timer.cpp
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green --progress-dir=/Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/CMakeFiles --progress-num=$(CMAKE_PROGRESS_7) "Building CXX object utils/CMakeFiles/utils-objs.dir/timer.cpp.o"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++  $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -o CMakeFiles/utils-objs.dir/timer.cpp.o -c /Users/oruqimaru/Desktop/FYP/Grasper/utils/timer.cpp

utils/CMakeFiles/utils-objs.dir/timer.cpp.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing CXX source to CMakeFiles/utils-objs.dir/timer.cpp.i"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -E /Users/oruqimaru/Desktop/FYP/Grasper/utils/timer.cpp > CMakeFiles/utils-objs.dir/timer.cpp.i

utils/CMakeFiles/utils-objs.dir/timer.cpp.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling CXX source to assembly CMakeFiles/utils-objs.dir/timer.cpp.s"
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && /Library/Developer/CommandLineTools/usr/bin/c++ $(CXX_DEFINES) $(CXX_INCLUDES) $(CXX_FLAGS) -S /Users/oruqimaru/Desktop/FYP/Grasper/utils/timer.cpp -o CMakeFiles/utils-objs.dir/timer.cpp.s

utils-objs: utils/CMakeFiles/utils-objs.dir/console_util.cpp.o
utils-objs: utils/CMakeFiles/utils-objs.dir/global.cpp.o
utils-objs: utils/CMakeFiles/utils-objs.dir/hdfs_core.cpp.o
utils-objs: utils/CMakeFiles/utils-objs.dir/mkl_util.cpp.o
utils-objs: utils/CMakeFiles/utils-objs.dir/mpi_unique_namer.cpp.o
utils-objs: utils/CMakeFiles/utils-objs.dir/tid_mapper.cpp.o
utils-objs: utils/CMakeFiles/utils-objs.dir/timer.cpp.o
utils-objs: utils/CMakeFiles/utils-objs.dir/build.make

.PHONY : utils-objs

# Rule to build all files generated by this target.
utils/CMakeFiles/utils-objs.dir/build: utils-objs

.PHONY : utils/CMakeFiles/utils-objs.dir/build

utils/CMakeFiles/utils-objs.dir/clean:
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils && $(CMAKE_COMMAND) -P CMakeFiles/utils-objs.dir/cmake_clean.cmake
.PHONY : utils/CMakeFiles/utils-objs.dir/clean

utils/CMakeFiles/utils-objs.dir/depend:
	cd /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /Users/oruqimaru/Desktop/FYP/Grasper /Users/oruqimaru/Desktop/FYP/Grasper/utils /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils /Users/oruqimaru/Desktop/FYP/Grasper/cmake-build-debug/utils/CMakeFiles/utils-objs.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : utils/CMakeFiles/utils-objs.dir/depend
