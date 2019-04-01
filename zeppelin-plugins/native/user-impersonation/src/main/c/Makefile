#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CC = gcc
CFLAGS  = -Wall -g
INCLUDES = -I/usr/include -I.
LFLAGS =
LIBS =
SRCS=$(wildcard *.c)
OBJS = $(patsubst %.c,%.o,$(SRCS))
OBJ = $(TARGET_DIR)/$(OBJS)
TARGET_DIR = ../../../target/bin
MAIN = execute-as-user

.PHONY: depend clean

all: $(MAIN)
	@echo "Successfully compiled user impersonation project"

$(MAIN): $(OBJS)
	$(CC) $(CFLAGS) $(INCLUDES) -o $(TARGET_DIR)/$(MAIN) $(OBJ) $(LFLAGS) $(LIBS)

.c.o:
	$(CC) $(CFLAGS) $(INCLUDES) -c $< -o $(TARGET_DIR)/$@

clean:
	$(RM) $(TARGET_DIR)/*.o $(TARGET_DIR)/$(MAIN)

depend: $(SRCS)
	makedepend $(INCLUDES) $^
