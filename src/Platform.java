import java.lang.reflect.Field;

import sun.misc.Unsafe;

public class Platform {
	public static Unsafe getUnsafeInstance() throws NoSuchFieldException,
		SecurityException, IllegalArgumentException, IllegalAccessException {
		Field unsafeInstance = Unsafe.class.getDeclaredField("theUnsafe");
		unsafeInstance.setAccessible(true);
		return (Unsafe) unsafeInstance.get(Unsafe.class);
	}
}
