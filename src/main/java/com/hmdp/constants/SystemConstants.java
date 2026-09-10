package com.hmdp.constants;

import java.io.File;

public class SystemConstants {
    public static final String IMAGE_UPLOAD_DIR = System.getProperty("user.dir")
            + File.separator + "front"
            + File.separator + "nginx-1.18.0"
            + File.separator + "html"
            + File.separator + "hmdp"
            + File.separator + "imgs";
    public static final String USER_NICK_NAME_PREFIX = "user_";
    public static final int DEFAULT_PAGE_SIZE = 5;
    public static final int MAX_PAGE_SIZE = 10;
}
