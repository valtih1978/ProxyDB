import scala.language.{existentials, reflectiveCalls}


object Utils {
	//def using[Resource <: Closeable, T](resource: Resource)(code: Resource => T) = try code(resource) finally resource.close
	def closing[A, B <: {def close(): Unit}] (closeable: B) (f: B => A): A =
	  try { f(closeable) } finally { closeable.close() }
	def pass[T](a: T)(f: T => Unit) = {f(a) ; a}
	def ?[T](sel: Boolean, a: => T, b: => T) = if (sel) a else b
	def id(obj: Any) = if (obj == null) "null" else obj.getClass.getSimpleName + "" + System.identityHashCode(obj)
	def class4name(name: String) = Class.forName(name, false, getClass.getClassLoader)
	var cacheSize = 2
 	def defaultDir = new java.io.File("""C:\Users\valentin\AppData\Local\Temp\zdb_swap7797261140896837624""")

	// hold no more than 2 objects in mem
	def withCleanDB[T](code: Db => T) = closing(Db(true)){code}
	def withExistingDB[T](code: Db => T) = closing(Db(false)){code}
	def Db(clean: Boolean) = new Db(defaultDir, cacheSize, clean)
	
	val formatter = java.text.NumberFormat.getIntegerInstance
	def decimal(l: Number) = formatter.format(l)

	def parse(s: String) = {
		val lc = s.toLowerCase()
		lc.filter(_.isDigit).toInt *
			?(lc.contains("k"), 1000, 1) *
			?(lc.contains("m"), 1000*1000, 1) *
			?(lc.contains("g"), 1000* 1000*1000, 1)
	}
	def eqa[T](a: T, b: T) = assert(a == b, s"$a != $b")


	import java.lang.reflect.{Field, Method, InvocationHandler, Proxy}
	
	/*
	import scala.reflect.runtime.{ universe => ru }
	  val pValue = ru.typeOf[Proxy].declaration(ru.TermName("h")).asTerm
	  val m = ru.runtimeMirror(/*p.*/getClass.getClassLoader)
  def updateInvocationHandler(p: Proxy, newIH: InvocationHandler) {
		val im = m.reflect(p) ; val pvMirror = im.reflectField(pValue) ; pvMirror.set(newIH)
	}*/
	  
	val hf = classOf[Proxy].getDeclaredField("h") ; hf.setAccessible(true);
	def updateInvocationHandler(p: Proxy, newIH: InvocationHandler) {
		hf.set(p, newIH)
  }
 }