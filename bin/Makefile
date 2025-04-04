# 这个make文件构建list_trace：一个应用程序来读取Slurm模拟器跟踪的内容。
# 要工作，需要在它们的版本中存在list_trace.c、sim_trace.c和sim_trace.h，即ScSF Slurm模拟器的0.1版本。
# 该文件不包括在此版本中，以避免许可证问题。

CC=gcc
CFLAGS=-DSLURM_SIMULATOR=1 -I.
DEPS = sim_trace.h


%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

list_trace: sim_trace.o list_trace.o
	gcc -o list_trace sim_trace.o list_trace.o $(CFLAGS)