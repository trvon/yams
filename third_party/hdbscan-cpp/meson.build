hdbscan_sources = files(
  'HDBSCAN-CPP/Distance/EuclideanDistance.cpp',
  'HDBSCAN-CPP/Distance/IDistanceCalculator.cpp',
  'HDBSCAN-CPP/Distance/ManhattanDistance.cpp',
  'HDBSCAN-CPP/Hdbscan/hdbscan.cpp',
  'HDBSCAN-CPP/HdbscanStar/cluster.cpp',
  'HDBSCAN-CPP/HdbscanStar/hdbscanAlgorithm.cpp',
  'HDBSCAN-CPP/HdbscanStar/hdbscanConstraint.cpp',
  'HDBSCAN-CPP/HdbscanStar/outlierScore.cpp',
  'HDBSCAN-CPP/HdbscanStar/undirectedGraph.cpp',
  'HDBSCAN-CPP/Runner/hdbscanParameters.cpp',
  'HDBSCAN-CPP/Runner/hdbscanResult.cpp',
  'HDBSCAN-CPP/Runner/hdbscanRunner.cpp',
  'HDBSCAN-CPP/Utils/bitSet.cpp',
)

hdbscan_inc = include_directories('HDBSCAN-CPP', is_system: true)

hdbscan_lib = static_library(
  'yams_hdbscan_cpp',
  hdbscan_sources,
  include_directories: hdbscan_inc,
  install: false,
  cpp_args: [
    '-Wno-unused-parameter',
    '-Wno-unused-variable',
    '-Wno-unused-but-set-variable',
    '-Wno-sign-compare',
    '-Wno-reorder',
    '-Wno-shadow',
    '-Wno-deprecated-declarations',
  ],
)

hdbscan_dep = declare_dependency(
  include_directories: hdbscan_inc,
  link_with: hdbscan_lib,
)
