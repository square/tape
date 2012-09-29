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

  private boolean running;

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
      running = true;
      task.execute(this);
    } else {
      Log.i(TAG, "Service starting!");
      stopSelf(); // No more tasks are present. Stop.
    }
  }

  @Override public void onSuccess(final String url) {
    running = false;
    queue.remove();
    bus.post(new ImageUploadSuccessEvent(url));
    executeNext();
  }

  @Override public void onFailure() {
  }

  @Override public IBinder onBind(Intent intent) {
    return null;
  }
}
