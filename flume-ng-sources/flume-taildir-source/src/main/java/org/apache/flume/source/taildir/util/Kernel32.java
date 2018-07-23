/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.flume.source.taildir.util;

/**
 * Created by caolch on 2017/4/25.
 */
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Structure;
import com.sun.jna.platform.win32.WinBase.FILETIME;
import com.sun.jna.platform.win32.WinDef.DWORD;
import com.sun.jna.platform.win32.WinNT.HANDLE;
import com.sun.jna.win32.StdCallLibrary;
import com.sun.jna.win32.W32APIFunctionMapper;
import com.sun.jna.win32.W32APITypeMapper;

public interface Kernel32 extends StdCallLibrary {
    final static Map<String, Object> WIN32API_OPTIONS = new HashMap<String, Object>() {
        private static final long serialVersionUID = 1L;
        {
            put(Library.OPTION_FUNCTION_MAPPER, W32APIFunctionMapper.UNICODE);
            put(Library.OPTION_TYPE_MAPPER, W32APITypeMapper.UNICODE);
        }
    };

    public Kernel32 INSTANCE = (Kernel32) Native.loadLibrary("Kernel32",
            Kernel32.class, WIN32API_OPTIONS);

    public int GetLastError();

    public class BY_HANDLE_FILE_INFORMATION extends Structure {
        public DWORD dwFileAttributes;
        public FILETIME ftCreationTime;
        public FILETIME ftLastAccessTime;
        public FILETIME ftLastWriteTime;
        public DWORD dwVolumeSerialNumber;
        public DWORD nFileSizeHigh;
        public DWORD nFileSizeLow;
        public DWORD nNumberOfLinks;
        public DWORD nFileIndexHigh;
        public DWORD nFileIndexLow;

        public static class ByReference extends BY_HANDLE_FILE_INFORMATION
                implements Structure.ByReference {

        };

        public static class ByValue extends BY_HANDLE_FILE_INFORMATION
                implements Structure.ByValue {

        }

        @Override
        protected List getFieldOrder() {
            List<String> fields = new ArrayList<String>();
            fields.addAll(Arrays.asList(new String[] { "dwFileAttributes",
                    "ftCreationTime", "ftLastAccessTime", "ftLastWriteTime",
                    "dwVolumeSerialNumber", "nFileSizeHigh", "nFileSizeLow",
                    "nNumberOfLinks", "nFileIndexHigh", "nFileIndexLow" }));
            return fields;

        };
    };

    boolean GetFileInformationByHandle(HANDLE hFile,
                                       BY_HANDLE_FILE_INFORMATION lpFileInformation);
}