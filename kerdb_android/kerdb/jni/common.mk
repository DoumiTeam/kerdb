
LOCAL_PATH := $(call my-dir)

RELATIVE_PATH := ../../../kerdb/leveldb
RELATIVE_PATH_SNAPPY := ../../../kerdb/snappy

C_INCLUDES = $(LOCAL_PATH) $(LOCAL_PATH)/include $(RELATIVE_PATH)/ $(RELATIVE_PATH_SNAPPY) $(RELATIVE_PATH)/include


SOURCES = \
	$(RELATIVE_PATH)/db/builder.cc \
	$(RELATIVE_PATH)/db/c.cc \
	$(RELATIVE_PATH)/db/db_impl.cc \
	$(RELATIVE_PATH)/db/db_iter.cc \
	$(RELATIVE_PATH)/db/dbformat.cc \
	$(RELATIVE_PATH)/db/dumpfile.cc \
	$(RELATIVE_PATH)/db/filename.cc \
	$(RELATIVE_PATH)/db/log_reader.cc \
	$(RELATIVE_PATH)/db/log_writer.cc \
	$(RELATIVE_PATH)/db/memtable.cc \
	$(RELATIVE_PATH)/db/repair.cc \
	$(RELATIVE_PATH)/db/table_cache.cc \
	$(RELATIVE_PATH)/db/version_edit.cc \
	$(RELATIVE_PATH)/db/version_set.cc \
	$(RELATIVE_PATH)/db/write_batch.cc \
	$(RELATIVE_PATH)/table/block_builder.cc \
	$(RELATIVE_PATH)/table/block.cc \
	$(RELATIVE_PATH)/table/filter_block.cc \
	$(RELATIVE_PATH)/table/format.cc \
	$(RELATIVE_PATH)/table/iterator.cc \
	$(RELATIVE_PATH)/table/merger.cc \
	$(RELATIVE_PATH)/table/table_builder.cc \
	$(RELATIVE_PATH)/table/table.cc \
	$(RELATIVE_PATH)/table/two_level_iterator.cc \
	$(RELATIVE_PATH)/util/arena.cc \
	$(RELATIVE_PATH)/util/bloom.cc \
	$(RELATIVE_PATH)/util/cache.cc \
	$(RELATIVE_PATH)/util/coding.cc \
	$(RELATIVE_PATH)/util/comparator.cc \
	$(RELATIVE_PATH)/util/crc32c.cc \
	$(RELATIVE_PATH)/util/env_posix.cc \
	$(RELATIVE_PATH)/util/env.cc \
	$(RELATIVE_PATH)/util/filter_policy.cc \
	$(RELATIVE_PATH)/util/hash.cc \
	$(RELATIVE_PATH)/util/histogram.cc \
	$(RELATIVE_PATH)/util/logging.cc \
	$(RELATIVE_PATH)/util/options.cc \
	$(RELATIVE_PATH)/util/status.cc \
	$(RELATIVE_PATH)/helpers/memenv/memenv.cc \
	$(RELATIVE_PATH_SNAPPY)/snappy-sinksource.cc \
	$(RELATIVE_PATH_SNAPPY)/snappy-stubs-internal.cc \
	$(RELATIVE_PATH_SNAPPY)/snappy.cc \
