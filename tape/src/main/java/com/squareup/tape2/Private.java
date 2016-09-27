package com.squareup.tape2;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Indicates that the given field or method has package visibility solely to prevent the creation of
 * a synthetic method. In practice, you should treat this field/method as if it were private. <p>
 *
 * When a private method is called from an inner class, the Java compiler generates a simple package
 * private shim method that the class generated from the inner class can call. This results in
 * unnecessary bloat and runtime method call overhead. It also gets us closer to the dex method
 * count limit. <p>
 *
 * If you'd like to see warnings for these synthetic methods in IntelliJ, turn on the inspections
 * "Private method only used from inner class" and "Private member access between outer and inner
 * classes". <p>
 */
@Retention(RetentionPolicy.SOURCE)
@Target({ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR, ElementType.TYPE})
@interface Private {
}
