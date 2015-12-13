
import Utils._, java.nio._, java.io._, java.nio.channels.FileChannel

// Here we have a large file of longs because we want to map sequential
// integers to some long values. Sequential generation of index file is thouthand times
// faster using memory-mapped buffers, as well as random access within 1GB file.
// However they are slightly slower, as 5 ms vs 4.7 ms, at random access to file that is
// larger than physical memory size.

// Here, 120m words = 1GB file
	// MmfDemo rnd_raf 120m 1m   // finished in 12 sec, i.e. 12 us/access
	// MmfDemo rnd_buf 120m 100m   // finished in 14 sec, i.e 140 ns/op or 100 times faster.

	// MmfDemo rnd_buf 1000m 100m  // finished in 20 sec
	// Running MmfDemo rnd_raf 1000m 1600k  // finished in 20 sec // 1/1.6 = 62.5 times slower


object MmfDemo extends App {

	val f = """C:\Users\valentin\AppData\Local\Temp\zdb_swap7797261140896837624\mmf"""
		
	if (args.length < 1) {
		println ("usage1: buf_gen <len in long words>")
		println ("usage1: raf_gen <len in long words>")
		println("example: buf_gen 30m")
		println("usage2: rd_raf <size in words>")
		println("usage2: rd_buf <size in words>")
		println("usage3: rnd_buf <file size in words> <samples>")
		println("usage3: rnd_raf <file size in words> <samples>")
		println("example: rnd_buf 1000m 1m")
		
	} else {
		def gen(index: RafIndex) = closing(index){ index =>
			index.raf.setLength(0)
			(0 until parse(args(1))) foreach {i => index.append(i.toLong)}
		}
		 
		def rdAll(index: RafIndex) = closing(index){ index =>
			var firstMismatch = -1
			val failures = (0 until parse(args(1))).foldLeft(0) { case(failures, i) =>
				val got = index(i)
				if (got != i && firstMismatch == -1) {firstMismatch = i; println("first mismatch at " +decimal(i) + ", value = " + decimal(got))}
				failures + ?(got != i, 1, 0)
			} ; println(decimal(failures) + " mismatches")
		}
	
		def random_access(index: RafIndex) = closing(index){ index =>
			val fileSize = parse(args(1)) ; val samples = parse(args(2))
			println((1 to samples).foldLeft(0) {
				case(failures, s) =>
					val addr = (Math.random * fileSize).toInt
					val got = index(addr)
					failures + ?(got != addr, 1, 0)
				} + " failures detected")
		}
		def raf = new RafIndex(f) ; def buf = new MmfIndex(f)
		val t1 = System.currentTimeMillis
		args(0) match {
			case "raf_gen" => gen(raf)
			case "buf_gen" => gen(buf)
			case "rd_raf" => rdAll{raf}
			case "rd_buf" => rdAll(buf)
			case "rnd_raf" => random_access{raf}
			case "rnd_buf" => random_access(buf)
			case u =>println("unknown command " + u)
		} ; println("finished in " + decimal(System.currentTimeMillis - t1) + " ms")
	}
	
}

class RafIndex(fn: String) extends Closeable {
	val raf = new RandomAccessFile(fn, "rw")
	def append(l: Long) = {this(raf.length.toInt) = l ; this}
	def update(pos: Int, value: Long) = {raf.seek(pos) ; raf.writeLong(value) }
	def apply(pos: Int) = {raf.seek(pos.toLong << 3) ; raf.readLong }
	def close = raf.close
}

class MmfIndex(fn: String) extends RafIndex(fn) {
	val fc = raf.getChannel
	val bufAddrWidth = 20 /*in bits*/ // Every element of the buff addresses a long
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
	override def apply(address: Int = pos) = {
		pos = address +1
		select(address).get
	}
	override def append(value: Long) = {this(pos) = value; this}
	override def update(pos: Int, value: Long) = {
		select(pos).put(value) ; this.pos += 1
	}
	def expand = {
		val fromByte = buffers.length.toLong  * BUF_SIZE_BYTES
		println("adding " + buffers.length + "th buffer, total size expected " + decimal(fromByte + BUF_SIZE_BYTES) + " bytes")
		
		// 32bit JVM: java.io.IOException: "Map failed" at the following line if buf size requested is larger than 512 mb
		// 64bit JVM: IllegalArgumentException: Size exceeds Integer.MAX_VALUE
		buffers :+= fc.map(FileChannel.MapMode.READ_WRITE, fromByte, BUF_SIZE_BYTES).asLongBuffer()
		capacity += BUF_SIZE_WORDS
	}
	override def close = {
		//fc.force(true) // this does not make sense
		fc.close ; super.close
		buffers = null ; System.gc /*GC needs to close the buffer*/
	}

}