package io.scalecube.transport.udp;

import com.google.common.base.Throwables;

import io.netty.util.internal.SystemPropertyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Locale;

public class EpollUtils {

  private static boolean envSupportEpoll;

  private static final Logger LOGGER = LoggerFactory.getLogger(EpollUtils.class);

  static {
    String name = SystemPropertyUtil.get("os.name").toLowerCase(Locale.UK).trim();
    if (!name.contains("linux")) {
      envSupportEpoll = false;
      LOGGER.warn("Env doesn't support epoll transport");
    } else {
      try {
        Class.forName("io.netty.channel.epoll.Native");
        envSupportEpoll = true;
        LOGGER.info("Use epoll transport");
      } catch (Throwable t) {
        LOGGER
            .warn("Tried to use epoll transport, but it's not supported by host OS (or no corresponding libs included) "
                + "using NIO instead, cause: " + Throwables.getRootCause(t));
        envSupportEpoll = false;
      }
    }
  }

  public static boolean isEpollSuported() {
    return envSupportEpoll;
  }
}
