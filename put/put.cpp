/* Copyright 2019 Husky Data Lab, CUHK

Authors: Hongzhi Chen (hzchen@cse.cuhk.edu.hk)
*/

#define USE_HDFS2_

#include "utils/hdfs_core.hpp"
#include "utils/global.hpp"

int main(int argc, char** argv)
{
	load_hdfs_config();
	const char* input = argv[1];
	const char* output = argv[2];
	put(input, output);
	return 0;
}
