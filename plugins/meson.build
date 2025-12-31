plugins_dir = get_option('libdir') / 'yams' / 'plugins'

if get_option('plugin-s3')
  subdir('object_storage_s3')
endif

if get_option('plugin-onnx')
  subdir('onnx')
endif

if get_option('plugin-symbols')
  subdir('symbol_extractor_treesitter')
endif

if get_option('plugin-zyp')
  subdir('zyp')
endif

