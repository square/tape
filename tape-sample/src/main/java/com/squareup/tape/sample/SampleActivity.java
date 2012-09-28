// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

import android.app.Activity;
import android.content.Intent;
import android.database.Cursor;
import android.os.Bundle;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.ArrayAdapter;
import android.widget.ListView;
import android.widget.TextView;
import android.widget.Toast;
import com.squareup.otto.Bus;
import com.squareup.otto.Subscribe;

import javax.inject.Inject;
import java.io.File;

import static android.content.Intent.ACTION_PICK;
import static android.provider.MediaStore.MediaColumns.DATA;
import static android.widget.Toast.LENGTH_SHORT;

public class SampleActivity extends Activity {
  private static final int PICK_IMAGE = 4 + 8 + 15 + 16 + 23 + 42;

  @Inject ImageUploadTaskQueue queue; // NOTE: Injection starts queue processing!
  @Inject Bus bus;

  private TextView status;
  private ArrayAdapter<String> uploads;

  @Override protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);

    ((SampleApplication) getApplication()).inject(this);

    setContentView(R.layout.sample_activity);

    // Status text reports number of pending uploads in the queue.
    status = (TextView) findViewById(R.id.status);

    // Hook up adapter to list of uploaded images.
    uploads = new ArrayAdapter<String>(this, R.layout.upload, android.R.id.text1);
    ListView uploadList = (ListView) findViewById(R.id.uploads);
    uploadList.setAdapter(uploads);

    // Upload button delegates to the gallery for selecting an image.
    findViewById(R.id.upload).setOnClickListener(new OnClickListener() {
      @Override public void onClick(View view) {
        Intent pickImageIntent = new Intent(ACTION_PICK);
        pickImageIntent.setType("image/*");
        startActivityForResult(pickImageIntent, PICK_IMAGE);
      }
    });
  }

  @Override protected void onActivityResult(int requestCode, int resultCode, Intent data) {
    if (requestCode == PICK_IMAGE && resultCode == RESULT_OK) {
      // Fetch the path to the selected image.
      Cursor cursor = getContentResolver().query(data.getData(), new String[] { DATA }, null, null, null);
      cursor.moveToFirst();
      File image = new File(cursor.getString(cursor.getColumnIndex(DATA)));
      cursor.close();

      // Add the image upload task to the queue.
      queue.add(new ImageUploadTask(image));
      Toast.makeText(this, R.string.task_added, LENGTH_SHORT).show();
    }
  }

  @SuppressWarnings("UnusedDeclaration") // Used by event bus.
  @Subscribe public void onQueueSizeChanged(ImageUploadQueueSizeEvent event) {
    status.setText(getString(R.string.status, event.size));
  }

  @SuppressWarnings("UnusedDeclaration") // Used by event bus.
  @Subscribe public void onUploadSuccess(ImageUploadSuccessEvent event) {
    Toast.makeText(this, R.string.task_completed, LENGTH_SHORT).show();
    uploads.add(event.url);
  }

  @Override protected void onResume() {
    super.onResume();
    bus.register(this); // Register for events when we are becoming the active activity.
  }

  @Override protected void onPause() {
    super.onPause();
    bus.unregister(this); // Unregister from events when we are no longer active.
  }
}
