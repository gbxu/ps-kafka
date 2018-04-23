#---------------------------------------------------------------------------------------
#  parameter server configuration script
#
#  include ps.mk after the variables are set
#
#----------------------------------------------------------------------------------------

ifeq ($(USE_KEY32), 1)
ADD_CFLAGS += -DUSE_KEY32=1
endif

PS_LDFLAGS_SO = -L$(DEPS_PATH)/lib -lprotobuf-lite -lrdkafka -lz -lpthread -lrt -ldl
PS_LDFLAGS_A = $(addprefix $(DEPS_PATH)/lib/, libprotobuf-lite.a librdkafka.a)
