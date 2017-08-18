package org.sead.repositories.reference.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.input.CountingInputStream;

public class OREInputStreamHolder {
	File map = null;
	CountingInputStream cis = null;
	long curPos = 0l;

	public OREInputStreamHolder(File oremap) throws FileNotFoundException {
		map = oremap;
		reset();

	}

	public void reset() throws FileNotFoundException {
		if (cis != null) {
			IOUtils.closeQuietly(cis);
		}
		cis = new CountingInputStream(
				new BufferedInputStream(new FileInputStream(map), Math.min((int) map.length(), 1000000)));
		curPos = 0l;

	}

	public CountingInputStream getCis() {
		return cis;

	}

	public void setCis(CountingInputStream newCis) {
		cis = newCis;
	}

	public long getCurPos() {
		return curPos;

	}

	public void setCurPos(long newPos) {
		curPos = newPos;
	}
}
