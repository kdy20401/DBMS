.SUFFIXES: .c .o

CC=gcc

SRCDIR=src/
INC=include/
LIBS=lib/

# SRCS:=$(wildcard src/*.c)
# OBJS:=$(SRCS:.c=.o)

# main source file
TARGET_SRC:=$(SRCDIR)main.c
TARGET_OBJ:=$(SRCDIR)main.o

OBJECTS = $(SRCDIR)main.o $(SRCDIR)bpt.o $(SRCDIR)file.o

# Include more files if you write another source file.
SRCS_FOR_LIB:=$(SRCDIR)bpt.c $(SRCDIR)file.c
OBJS_FOR_LIB:=$(SRCS_FOR_LIB:.c=.o)

CFLAGS+= -g -fPIC -I $(INC)

TARGET=main

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CC) $(CFLAGS) -o $@ $(OBJECTS)
	make static_library
	$(CC) $(CFLAGS) -o $@ $^ -L $(LIBS) -lbpt

clean:
	rm $(TARGET) $(OBJECTS) $(LIBS)*

library:
	gcc -shared -Wl,-soname,libbpt.so -o $(LIBS)libbpt.so $(OBJS_FOR_LIB)

# make static library libbpt.a using following object files
static_library:
	ar cr $(LIBS)libbpt.a $(OBJS_FOR_LIB)


# dependency
$(SRCDIR)bpt.o : $(SRCDIR)bpt.c $(INC)bpt.h $(INC)file.h 
$(SRCDIR)file.o : $(SRCDIR)file.c $(INC)file.h
$(SRCDIR)main.o : $(SRCDIR)main.c $(INC)bpt.h $(INC)file.h