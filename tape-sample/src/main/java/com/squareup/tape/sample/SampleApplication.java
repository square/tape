// Copyright 2012 Square, Inc.
package com.squareup.tape.sample;

import android.app.Application;
import android.content.Context;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.squareup.otto.Bus;
import dagger.Module;
import dagger.ObjectGraph;
import dagger.Provides;

import javax.inject.Singleton;

public class SampleApplication extends Application {
  private ObjectGraph objectGraph;

  @Override public void onCreate() {
    super.onCreate();
    objectGraph = ObjectGraph.create(new SampleModule(this));
  }

  public void inject(Object object) {
    objectGraph.inject(object);
  }

  @Module(
      entryPoints = {
          SampleActivity.class, //
          ImageUploadTaskQueue.class, //
          ImageUploadTaskService.class //
      }
  )
  static class SampleModule {
    private final Context appContext;

    SampleModule(Context appContext) {
      this.appContext = appContext;
    }

    @Provides @Singleton ImageUploadTaskQueue provideTaskQueue(Gson gson, Bus bus) {
      return ImageUploadTaskQueue.create(appContext, gson, bus);
    }

    @Provides @Singleton Bus provideBus() {
      return new Bus();
    }

    @Provides @Singleton Gson provideGson() {
      return new GsonBuilder().create();
    }
  }
}
