// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

import android.os.Handler;
import android.os.Looper;
import android.util.Log;
import com.github.kevinsawicki.http.HttpRequest;
import com.squareup.tape.Task;

import java.io.File;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Random;

import static com.github.kevinsawicki.http.HttpRequest.post;

/** Uploads the specified file to imgur.com. */
public class ImageUploadTask implements Task<ImageUploadTask.Callback> {
  private static final long serialVersionUID = 126142781146165256L;

  private static final String TAG = "Tape:ImageUploadTask";
  private static final String IMGUR_API_KEY = "74e20e836f0307a90683c4643a2b656e";
  private static final String IMGUR_UPLOAD_URL = "http://api.imgur.com/2/upload";
  private static final Pattern IMGUR_URL_REGEX = Pattern.compile("<imgur_page>(.+?)</imgur_page>");
  private static final Handler MAIN_THREAD = new Handler(Looper.getMainLooper());
  private static final int RETRY_WAIT = 2000;

  public interface Callback {
    void onSuccess(String url);
    void onFailure();
  }

  private final File file;

  public ImageUploadTask(File file) {
    this.file = file;
  }

  @Override public void execute(final Callback callback) {
    // Image uploading is slow. Execute HTTP POST on a background thread.
    new Thread(new Runnable() {
      @Override public void run() {
        try {
          // Simulate a failure 1 in 2 times
          String apiKey = IMGUR_API_KEY;
          if (new Random().nextBoolean()) {
            apiKey = "foobar";
          }
          HttpRequest request = post(IMGUR_UPLOAD_URL)
              .part("key", apiKey)
              .part("image", file);

          if (request.ok()) {
            Matcher m = IMGUR_URL_REGEX.matcher(request.body());
            m.find();
            final String url = m.group(1);
            Log.i(TAG, "Upload success! " + url);

            // Get back to the main thread before invoking a callback.
            MAIN_THREAD.post(new Runnable() {
              @Override public void run() {
                callback.onSuccess(url);
              }
            });
          } else {
            Log.i(TAG, String.format(
                  "Upload failed :(  Will retry in %d seconds.",
                  RETRY_WAIT / 1000));

            try {
              Thread.sleep(RETRY_WAIT);
            } catch (InterruptedException e) {
              e.printStackTrace();
              throw new RuntimeException(e);
            }

            // Get back to the main thread before invoking a callback.
            MAIN_THREAD.post(new Runnable() {
              @Override public void run() {
                callback.onFailure();
              }
            });
          }
        } catch (RuntimeException e) {
          e.printStackTrace();
          throw e;
        }
      }
    }).start();
  }
}
