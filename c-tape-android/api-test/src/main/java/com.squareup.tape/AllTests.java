package com.squareup.tape;

import junit.framework.Test;
import junit.framework.TestSuite;

import android.test.suitebuilder.TestSuiteBuilder;

/**
 * A test suite containing all tests for ApiDemos.
 *
 * To run all suites found in this apk:
 * $ adb shell am instrument -w \
 *   com.squareup.tape.test/android.test.InstrumentationTestRunner
 *
 * To run just this suite from the command line:
 * $ adb shell am instrument -w \
 *   -e class com.example.android.apis.AllTests \
 *   com.squareup.tape.test/android.test.InstrumentationTestRunner
 *
 * To run an individual test case, e.g. {@link com.example.android.apis.os.MorseCodeConverterTest}:
 * $ adb shell am instrument -w \
 *   -e class com.example.android.apis.os.MorseCodeConverterTest \
 *   com.squareup.tape.test/android.test.InstrumentationTestRunner
 *
 * To run an individual test, e.g. {@link com.example.android.apis.os.MorseCodeConverterTest#testCharacterS()}:
 * $ adb shell am instrument -w \
 *   -e class com.example.android.apis.os.MorseCodeConverterTest#testCharacterS \
 *   com.squareup.tape.test/android.test.InstrumentationTestRunner
 */
public class AllTests extends TestSuite {

  public static Test suite() {
    return new TestSuiteBuilder(AllTests.class)
        .includeAllPackagesUnderHere()
        .build();
  }
}