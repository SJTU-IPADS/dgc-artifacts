#
# Copyright (c) 2024, Oracle and/or its affiliates. All rights reserved.
# DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
#
# This code is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 only, as
# published by the Free Software Foundation.  Oracle designates this
# particular file as subject to the "Classpath" exception as provided
# by Oracle in the LICENSE file that accompanied this code.
#
# This code is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
# version 2 for more details (a copy is included in the LICENSE file that
# accompanied this code).
#
# You should have received a copy of the GNU General Public License version
# 2 along with this work; if not, write to the Free Software Foundation,
# Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
#
# Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
# or visit www.oracle.com if you need additional information or have any
# questions.
#

################################################################################
# Check if a potential OR-Tools library match is correct and usable
################################################################################
AC_DEFUN([LIB_CHECK_POTENTIAL_ORTOOLS],
[
  POTENTIAL_ORTOOLS_INCLUDE_PATH="$1"
  POTENTIAL_ORTOOLS_LIB_PATH="$2"
  METHOD="$3"

  # Let's start with an optimistic view of the world :-)
  FOUND_ORTOOLS=yes

  # First look for the canonical OR-Tools main include file ortools/sat/cp_model.h.
  if ! test -s "$POTENTIAL_ORTOOLS_INCLUDE_PATH/ortools/sat/cp_model.h"; then
    # Fail.
    FOUND_ORTOOLS=no
  fi

  if test "x$FOUND_ORTOOLS" = "xyes"; then
    # Include file found, let's continue the sanity check.
    AC_MSG_NOTICE([Found OR-Tools include files at $POTENTIAL_ORTOOLS_INCLUDE_PATH using $METHOD])

    # Check for both shared and static libraries
    ORTOOLS_LIB_NAME="${LIBRARY_PREFIX}ortools${SHARED_LIBRARY_SUFFIX}"
    ORTOOLS_STATIC_LIB_NAME="${LIBRARY_PREFIX}ortools${STATIC_LIBRARY_SUFFIX}"
    
    # Check for shared library first
    if test -s "$POTENTIAL_ORTOOLS_LIB_PATH/$ORTOOLS_LIB_NAME"; then
      ORTOOLS_USE_SHARED=yes
      ORTOOLS_USE_STATIC=no
    elif test -s "$POTENTIAL_ORTOOLS_LIB_PATH/$ORTOOLS_STATIC_LIB_NAME"; then
      ORTOOLS_USE_SHARED=no
      ORTOOLS_USE_STATIC=yes
    else
      AC_MSG_NOTICE([Could not find $POTENTIAL_ORTOOLS_LIB_PATH/$ORTOOLS_LIB_NAME or $POTENTIAL_ORTOOLS_LIB_PATH/$ORTOOLS_STATIC_LIB_NAME. Ignoring location.])
      FOUND_ORTOOLS=no
    fi
  fi

  if test "x$FOUND_ORTOOLS" = "xyes"; then
    ORTOOLS_INCLUDE_PATH="$POTENTIAL_ORTOOLS_INCLUDE_PATH"
    AC_MSG_CHECKING([for OR-Tools includes])
    AC_MSG_RESULT([$ORTOOLS_INCLUDE_PATH])
    ORTOOLS_LIB_PATH="$POTENTIAL_ORTOOLS_LIB_PATH"
    AC_MSG_CHECKING([for OR-Tools libraries])
    AC_MSG_RESULT([$ORTOOLS_LIB_PATH])
    
    # Check for required static libraries
    REQUIRED_STATIC_LIBS="protobuf protobuf-lite re2 scip utf8_range utf8_validity z"
    ORTOOLS_STATIC_LIBS=""
    
    for lib in $REQUIRED_STATIC_LIBS; do
      STATIC_LIB_NAME="${LIBRARY_PREFIX}${lib}${STATIC_LIBRARY_SUFFIX}"
      if test -s "$POTENTIAL_ORTOOLS_LIB_PATH/$STATIC_LIB_NAME"; then
        ORTOOLS_STATIC_LIBS="$ORTOOLS_STATIC_LIBS -l$lib"
        AC_MSG_NOTICE([Found static library: $STATIC_LIB_NAME])
      else
        AC_MSG_WARN([Missing static library: $STATIC_LIB_NAME])
      fi
    done
    
    # Check for abseil libraries
    ABSL_LIBS="base flags hash int128 log synchronization time"
    for lib in $ABSL_LIBS; do
      ABSL_LIB_NAME="${LIBRARY_PREFIX}absl_${lib}${STATIC_LIBRARY_SUFFIX}"
      if test -s "$POTENTIAL_ORTOOLS_LIB_PATH/$ABSL_LIB_NAME"; then
        ORTOOLS_STATIC_LIBS="$ORTOOLS_STATIC_LIBS -labsl_$lib"
        AC_MSG_NOTICE([Found abseil library: $ABSL_LIB_NAME])
      else
        AC_MSG_WARN([Missing abseil library: $ABSL_LIB_NAME])
      fi
    done
    
    # Add more abseil libraries that might be needed
    ABSL_EXTRA_LIBS="flags_config flags_internal flags_marshalling flags_parse flags_private_handle_accessor flags_program_name flags_reflection flags_usage flags_usage_internal graphcycles_internal hashtablez_sampler kernel_timeout_internal leak_check log_entry log_flags log_globals log_initialize log_internal_check_op log_internal_conditions log_internal_fnmatch log_internal_format log_internal_globals log_internal_log_sink_set log_internal_message log_internal_nullguard log_internal_proto log_severity log_sink low_level_hash malloc_internal periodic_sampler random_distributions random_internal_distribution_test_util random_internal_platform random_internal_pool_urbg random_internal_randen random_internal_randen_hwaes random_internal_randen_hwaes_impl random_internal_randen_slow random_internal_seed_material random_seed_gen_exception random_seed_sequences raw_hash_set raw_logging_internal scoped_set_env spinlock_wait stacktrace status statusor str_format_internal strerror string_view strings strings_internal symbolize throw_delegate time_zone vlog_config_internal"
    
    for lib in $ABSL_EXTRA_LIBS; do
      ABSL_LIB_NAME="${LIBRARY_PREFIX}absl_${lib}${STATIC_LIBRARY_SUFFIX}"
      if test -s "$POTENTIAL_ORTOOLS_LIB_PATH/$ABSL_LIB_NAME"; then
        ORTOOLS_STATIC_LIBS="$ORTOOLS_STATIC_LIBS -labsl_$lib"
        AC_MSG_NOTICE([Found extra abseil library: $ABSL_LIB_NAME])
      fi
    done
  fi
])

################################################################################
# Setup OR-Tools (Google OR-Tools constraint programming library)
################################################################################
AC_DEFUN_ONCE([LIB_SETUP_ORTOOLS],
[
  AC_ARG_WITH(ortools, [AS_HELP_STRING([--with-ortools],
      [specify the OR-Tools installation directory.
       The directory should contain 'include' and 'lib' subdirectories.
       Example: --with-ortools=/usr/local/ortools])])

  ORTOOLS_BASE_NAME=ortools
  ORTOOLS_CFLAGS=
  ORTOOLS_LIBS=

  if test "x$with_ortools" != "x" ; then
    if test "x$with_ortools" = "xno" ; then
      # User explicitly disabled OR-Tools
      ORTOOLS_TO_USE=no
    else
      # User specified a path
      ORTOOLS_TO_USE=system
      POTENTIAL_ORTOOLS_INCLUDE_PATH="$with_ortools/include"
      POTENTIAL_ORTOOLS_LIB_PATH="$with_ortools/lib"
      
      # Check that the specified location works
      LIB_CHECK_POTENTIAL_ORTOOLS($POTENTIAL_ORTOOLS_INCLUDE_PATH,
          $POTENTIAL_ORTOOLS_LIB_PATH, [--with-ortools])
      if test "x$FOUND_ORTOOLS" != "xyes" ; then
        AC_MSG_ERROR([Can not find or use OR-Tools at location given by --with-ortools])
      fi
    fi
  else
    # User did not specify a location, default to not using OR-Tools
    ORTOOLS_TO_USE=no
    AC_MSG_ERROR([Can not find or use OR-Tools at location given by --with-ortools])
  fi

  if test "x$ORTOOLS_TO_USE" = "xsystem" ; then
    # Set ORTOOLS_CFLAGS, _LIBS and _LIB_PATH from include and lib dir.
    if test "x$ORTOOLS_CFLAGS" = "x" ; then
      ORTOOLS_CFLAGS="-I$ORTOOLS_INCLUDE_PATH"
    fi

    if test "x$ORTOOLS_LIBS" = "x" ; then
      if test "x$ORTOOLS_USE_STATIC" = "xyes" ; then
        # Use static libraries
        ORTOOLS_LIBS="-L$ORTOOLS_LIB_PATH -lortools $ORTOOLS_STATIC_LIBS"
        AC_MSG_NOTICE([Using OR-Tools static libraries])
      else
        # Use shared libraries
        ORTOOLS_LIBS="-L$ORTOOLS_LIB_PATH -lortools -lortools_flatzinc"
        AC_MSG_NOTICE([Using OR-Tools shared libraries])
      fi
    fi
  fi

  LIBS="$LIBS $ORTOOLS_LIBS"

  AC_MSG_RESULT([Using OR-Tools: $ORTOOLS_TO_USE])
  AC_MSG_RESULT([POTENTIAL_ORTOOLS_INCLUDE_PATH: $POTENTIAL_ORTOOLS_INCLUDE_PATH])
  AC_MSG_RESULT([POTENTIAL_ORTOOLS_LIB_PATH: $POTENTIAL_ORTOOLS_LIB_PATH])
  AC_MSG_RESULT([ORTOOLS_STATIC_LIBS: $ORTOOLS_STATIC_LIBS])

  AC_SUBST(ORTOOLS_TO_USE)
  AC_SUBST(ORTOOLS_CFLAGS)
  AC_SUBST(ORTOOLS_LIBS)
  AC_SUBST(ORTOOLS_LIB_PATH)
  AC_SUBST(ORTOOLS_STATIC_LIBS)
]) 