
SRCS=$(wildcard *.c)
OBJS=$(SRCS:.c=.o)
DEPS=$(SRCS:.c=.d)

TEST_SRCS=$(wildcard tests/*.c)
TEST_OBJS=$(TEST_SRCS:.c=.o)
TEST_DEPS=$(TEST_SRCS:.c=.d)

OPT_FLAGS=-O3

all: c-tape

debug: OPT_FLAGS=-ggdb
debug: all


c-tape: $(OBJS) $(TEST_OBJS)
	@echo 'Building target: $@'
	gcc  -o "c-tape" $(OBJS) $(TEST_OBJS)
	@echo 'Finished building target: $@'
	@echo ' '

%.o: %.c
	@echo 'Building file: $@'
	gcc $(OPT_FLAGS) -Wall -Wextra -Werror -Wconversion -c -fmessage-length=0 -Wno-unused-function -MMD -MP -MF"$(@:%.o=%.d)" -MT"$(@:%.o=%.d)" -o "$@" "$<"
	@echo 'Finished building: $@'
	@echo ' '

clean:
	rm -rf $(OBJS) $(DEPS) $(TEST_OBJS) $(TEST_DEPS) "c-tape"