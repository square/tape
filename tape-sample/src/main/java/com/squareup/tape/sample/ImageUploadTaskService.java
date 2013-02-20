// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;
import android.util.Log;
import com.squareup.otto.Bus;
import com.squareup.tape.sample.ImageUploadTask.Callback;

import javax.inject.Inject;

public class ImageUploadTaskService extends Service implements Callback {
  private static final String TAG = "Tape:ImageUploadTaskService";

  @Inject ImageUploadTaskQueue queue;
  @Inject Bus bus;

  private static final int MAX_RETRIES = 5;

  private boolean running;
  private int numRetries = 0;

  @Override public void onCreate() {
    super.onCreate();
    ((SampleApplication) getApplication()).inject(this);
    Log.i(TAG, "Service starting!");
  }

  @Override public int onStartCommand(Intent intent, int flags, int startId) {
    executeNext();
    return START_STICKY;
  }

  private void executeNext() {
    if (running) return; // Only one task at a time.

    ImageUploadTask task = queue.peek();
    if (task != null) {
      Log.i(TAG, "Attempt: " + numRetries);
      running = true;
      task.execute(this);
    } else {
      Log.i(TAG, "Service stopping!");
      stopSelf(); // No more tasks are present. Stop.
    }
  }

  @Override public void onSuccess(final String url) {
    running = false;
    queue.remove();
    bus.post(new ImageUploadSuccessEvent(url, numRetries));
    numRetries = 0;
    executeNext();
  }

  @Override public void onFailure() {
    numRetries++;
    running = false;
    if (numRetries >= MAX_RETRIES) {
      bus.post(new ImageUploadFailureEvent(numRetries));
      Log.i(TAG, "Service stopping!");
      stopSelf();
    } else {
      executeNext();
    }
  }

  @Override public IBinder onBind(Intent intent) {
    return null;
  }
}
