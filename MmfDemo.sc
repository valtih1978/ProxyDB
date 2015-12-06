
import Utils._, java.nio._, java.io._, java.nio.channels.FileChannel

// Here we have a large file of longs because we want to map sequential
// integers to some long values. Buffer reads 400x faster than RAF
	// Index rd_buf 400m  // finished in 12 sec
	// Index rd_raf 1m   // finished in 10 sec
// but it has problems: it is OS-dependent and closed by unreliable
// GC (I see no flush mechanism but buffer-written data appear on
// the disk magically in the end)

object MmfDemo extends App {

	val bufAddrWidth = 25 /*in bits*/ // Every element of the buff addresses a long
	val BUF_SIZE_WORDS = 1 << bufAddrWidth ; val BUF_SIZE_BYTES = BUF_SIZE_WORDS << 3
	val bufBitMask = BUF_SIZE_WORDS - 1
	var buffers = Vector[LongBuffer]()
	var capacity = 0 ; var pos = 0
	def select(pos: Int) = {
		val bufn = pos >> bufAddrWidth // higher bits of address denote the buffer number
		//println(s"accessing $pos = " + (pos - buf * wordsPerBuf) + " in " + buf)
		while (buffers.length <= bufn) expand
		pass(buffers(bufn)){_.position(pos & bufBitMask)}
	}
	def get(address: Int = pos) = {
		pos = address +1
		select(address).get
	}
	def put(value: Long) {
		//println("writing " + value + " to " + pos)
		select(pos).put(value) ; pos += 1
	}
	def expand = {
		val fromByte = buffers.length.toLong  * BUF_SIZE_BYTES
		println("adding " + buffers.length + "th buffer, total size expected " + format(fromByte + BUF_SIZE_BYTES) + " bytes")
		
		// 32bit JVM: java.io.IOException: "Map failed" at the following line if buf size requested is larger than 512 mb
		// 64bit JVM: IllegalArgumentException: Size exceeds Integer.MAX_VALUE
		buffers :+= fc.map(FileChannel.MapMode.READ_WRITE, fromByte, BUF_SIZE_BYTES).asLongBuffer()
		capacity += BUF_SIZE_WORDS
	}
					
	def rdAll(get: Int => Long) {
		var firstMismatch = -1
		val failures = (0 until parse(args(1))).foldLeft(0) { case(failures, i) =>
			val got = get(i)
			if (got != i && firstMismatch == -1) {firstMismatch = i; println("first mismatch at " +format(i) + ", value = " + format(got))}
			failures + ?(got != i, 1, 0)
		} ; println(format(failures) + " mismatches")
	}

	val raf = new RandomAccessFile("""C:\Users\valentin\AppData\Local\Temp\zdb_swap7797261140896837624\mmf""", "rw")
	val fc = raf.getChannel
	try {
		
		if (args.length < 1) {
			println ("usage1: buf_gen <len in long words>")
			println ("usage1: raf_gen <len in long words>")
			println("example: buf_gen 30m")
			println("usage2: raf_rd <size in words>")
			println("usage2: buf_rd <size in words>")
			println("usage3: rnd_buf <file size in words> <samples>")
			println("usage3: rnd_raf <file size in words> <samples>")
			println("example: rnd_buf 1000m 1m")
			
		} else {
			val t1 = System.currentTimeMillis
			args(0) match {
				case "buf_gen" => raf.setLength(0)
					(0 until parse(args(1))) foreach {i => put(i.toLong)}
				case "raf_gen" => raf.setLength(0)
					(0 until parse(args(1))) foreach {i =>raf.writeLong(i.toLong)}
						//fc.force(true)
				case "rd_raf" => rdAll{i => raf.seek(i.toLong * 8) ; raf.readLong()}
				case "rd_buf" => rdAll(get)
				case "rnd_buf" => random_access(get)
				case "rnd_raf" => random_access{i => raf.seek(i.toLong * 8) ; raf.readLong()}
				case u =>println("unknown command " + u)
			} ; println("finished in " + (System.currentTimeMillis - t1) + " ms")
		}
	} finally {
		raf.close ; fc.close

		buffers = null ; System.gc /*GC needs to close the buffer*/
	}
	
	def random_access(get: Int => Long) {
		val fileSize = parse(args(1)) ; val samples = parse(args(2))
		println((1 to samples).foldLeft(0) {
			case(failures, s) =>
				val addr = (Math.random * fileSize).toInt
				val got = get(addr)
				failures + ?(got != addr, 1, 0)
			} + " failures detected")
	}
}

object Utils {
	val formatter = java.text.NumberFormat.getIntegerInstance
	def format(l: Number) = formatter.format(l)

	def ?[T](sel: Boolean, a: => T, b: => T) = if (sel) a else b
	def parse(s: String) = {
		val lc = s.toLowerCase()
		lc.filter(_.isDigit).toInt *
			?(lc.contains("k"), 1000, 1) *
			?(lc.contains("m"), 1000*1000, 1)
	}
	def eqa[T](a: T, b: T) = assert(a == b, s"$a != $b")
	def pass[T](a: T)(code: T => Unit) = {code(a) ; a}
}