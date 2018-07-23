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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.WinBase;
import com.sun.jna.platform.win32.WinNT.HANDLE;
//import org.apache.flume.source.wintaildir.util.Kernel32;
public class WinFileUtil {
    private static Logger logger = LoggerFactory.getLogger(WinFileUtil.class);

    public static String getFileId(String filepath) {

        final int FILE_SHARE_READ = (0x00000001);
        final int OPEN_EXISTING = (3);
        final int GENERIC_READ = (0x80000000);
        final int FILE_ATTRIBUTE_ARCHIVE = (0x20);

        WinBase.SECURITY_ATTRIBUTES attr = null;
        org.apache.flume.source.taildir.util.Kernel32.BY_HANDLE_FILE_INFORMATION lpFileInformation = new org.apache.flume.source.taildir.util.Kernel32.BY_HANDLE_FILE_INFORMATION();
        HANDLE hFile = null;

        hFile = Kernel32.INSTANCE.CreateFile(filepath, 0,
                FILE_SHARE_READ, attr, OPEN_EXISTING, FILE_ATTRIBUTE_ARCHIVE,
                null);
        String ret = "0";
        if (Kernel32.INSTANCE.GetLastError() == 0) {

            org.apache.flume.source.taildir.util.Kernel32.INSTANCE
                    .GetFileInformationByHandle(hFile, lpFileInformation);

            ret = lpFileInformation.dwVolumeSerialNumber.toString()
                    + lpFileInformation.nFileIndexLow.toString();

            Kernel32.INSTANCE.CloseHandle(hFile);

            if (Kernel32.INSTANCE.GetLastError() == 0) {
                logger.debug("inode:" + ret);
                return ret;
            } else {
                logger.error("关闭文件发生错误:{}", filepath);
                throw new RuntimeException("关闭文件发生错误:" + filepath);
            }
        } else {
            if (hFile != null) {
                Kernel32.INSTANCE.CloseHandle(hFile);
            }
            logger.error("打开文件发生错误:{}", filepath);
            throw new RuntimeException("打开文件发生错误:" + filepath);
        }

    }
}