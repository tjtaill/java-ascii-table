/**
 * Copyright (C) 2011 K Venkata Sudhakar <kvenkatasudhakar@gmail.com>
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *         http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.bethecoder.ascii_table.impl;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.bethecoder.ascii_table.ASCIITableHeader;
import com.bethecoder.ascii_table.spec.IASCIITableAware;
import org.apache.commons.lang3.text.WordUtils;

/**
 * This class is useful to extract the header and row data from
 * JDBC ResultSet and SQLs.
 *  
 * @author K Venkata Sudhakar (kvenkatasudhakar@gmail.com)
 * @version 1.0
 *
 */
public class JDBCASCIITableAware implements IASCIITableAware {

	private List<ASCIITableHeader> headers = null;
	private List<List<Object>> data = null;
	private final int maxColumnWidth;
	
	public JDBCASCIITableAware(Connection connection, String sql, int maxColumnWidth) {
		this.maxColumnWidth = maxColumnWidth;
		try {
			Statement stmt = connection.createStatement();
			ResultSet resultSet = stmt.executeQuery(sql);
			init(resultSet);
		} catch (SQLException e) {
			throw new RuntimeException("Unable to get table data : " + e);
		}
	}
	
	public JDBCASCIITableAware(ResultSet resultSet, int maxColumnWidth) {
		this.maxColumnWidth = maxColumnWidth;
		try {
			init(resultSet);
		} catch (SQLException e) {
			throw new RuntimeException("Unable to get table data : " + e);
		}
	}

	private void init(ResultSet resultSet) throws SQLException {
		
		//Populate header
		int colCount = resultSet.getMetaData().getColumnCount();
		headers = new ArrayList<ASCIITableHeader>(colCount);
		for (int i = 0 ; i < colCount ; i ++) {
			headers.add(new ASCIITableHeader(
					resultSet.getMetaData().getColumnLabel(i + 1).toUpperCase()));
		}


		
		//Populate data
		data = new ArrayList<List<Object>>();
		List<Object> rowData = null;
		List<Object> tempData;
		while (resultSet.next()) {
			boolean isAnyColumnMultiline = false;
			boolean[] columnHasMultiline = new boolean[colCount];
			
			rowData = new ArrayList<Object>();

			// figure out if any of the column values need to be split across
			// multiple lines
			for (int i = 0 ; i < colCount ; i ++) {
				Object object = resultSet.getObject(i + 1);
				String val = String.valueOf(object);
				if ( val.contains("\n") || val.length() > maxColumnWidth ) {
					columnHasMultiline[i] = true;
					isAnyColumnMultiline = true;
				}
				rowData.add(object);
			}

			if ( isAnyColumnMultiline ) {
				// create extra as many extra rows as needed to format
				// long strings and multiline string
				int maxRows = 2;
				Object[][] columns = new Object[colCount][];
				for(int i = 0; i < colCount; i++) {
					if ( ! columnHasMultiline[i] ) continue;

					String val = String.valueOf( rowData.get(i) );
					String[] vals = null;
					if ( val.contains("\n") ) {
						vals = val.split("\n");
					} else if ( val.length() > maxColumnWidth ) {
						String wrap = WordUtils.wrap(val, maxColumnWidth);
						vals = wrap.split("\n");
					}
					columns[i] = vals;
					maxRows = Math.max(maxRows, vals.length);
				}

				for(int i = 0; i < colCount; i++) {
					if ( columns[i] == null ) {
						columns[i] = padedColumn( rowData.get(i), maxRows );
					} else if ( columns[i].length < maxRows ) {
						Object[] padedArray = new Object[maxRows];
						for (int j = 0; j < maxRows; j++) {
							Object val = j < columns[i].length ? columns[i][j] : "";
							padedArray[j] = val;
						}
						columns[i] = padedArray;
					}

				}
				for( int r = 0; r < maxRows; r++ ) {
					rowData.clear();
					for(int c = 0; c < colCount; c++) {
						rowData.add( columns[c][r] );
					}
					data.add( rowData );
				}
			} else {
				data.add(rowData);
			}
		}//iterate rows
		
	}

	private Object[] padedColumn(Object columnVal, int maxRows) {
		Object[] column = new Object[maxRows];
		column[0] = columnVal;
		for (int i = 1; i < maxRows; i++) {
			column[i] = "";
		}
		return column;
	}
	
	@Override
	public List<List<Object>> getData() {
		return data;
	}

	@Override
	public List<ASCIITableHeader> getHeaders() {
		return headers;
	}

	@Override
	public String formatData(ASCIITableHeader header, int row, int col, Object data) {
		//Format only numbers
		try {
			BigDecimal bd = new BigDecimal(data.toString());
			return DecimalFormat.getInstance().format(bd);
		} catch (Exception e) {
		}

		//For non-numbers return null 
		return null;
	}
}
