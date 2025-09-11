cmake_minimum_required(VERSION 3.20)
project(yams_s3_plugin C)
add_library(yams_s3_plugin SHARED src/s3_stub_plugin.c)
set_target_properties(yams_s3_plugin PROPERTIES OUTPUT_NAME "yams_s3_plugin")
