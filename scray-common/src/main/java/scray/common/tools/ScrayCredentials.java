package scray.common.tools;

/**
 * container class to keep usernames and passwords
 * @author andreas.petter@gmail.com
 *
 */
public class ScrayCredentials {
	
	private String username = null;
	private char[] password = null;
	
	public ScrayCredentials() {
		username = null;
		this.password = null;
	}
	
	public ScrayCredentials(String name, char[] password) {
		username = name;
		this.password = password;
	}
	
	public boolean isEmpty() {
		return (username == null || username.trim().equals("")) && (password == null || password.length == 0);
	}
	
	public String getUsername() {
		return username;
	}
	
	public char[] getPassword() {
		return password;
	}
	
	/**
	 * overrides and removes the password from memory, such that it cannot be
	 * read from main memory by malicious code. Of course, this method can only
	 * be used after the password has reached the end of its life cycle.
	 */
	public void clearPassword() {
		if(password != null) {
			for(int i = 0; i < password.length; i ++) {
				password[i] = 0;
			}
			password = null;
		}
	}
	
	@Override
	protected void finalize() {
		clearPassword();
	}
	
	@Override
	public boolean equals(Object other) {
		if(other != null && other instanceof ScrayCredentials) {
			ScrayCredentials creds = (ScrayCredentials) other;
			if(isEmpty() && creds.isEmpty()) {
				return true;
			}
			boolean userEquals = false;
			if(getUsername() != null) {
				userEquals = getUsername().equals(creds.getUsername());
			} else {
				if(getUsername() == null && creds.getUsername() == null) {
					userEquals = true;
				}
			}
			boolean pwdEquals = false;
			if(getPassword() != null) {
				if(creds.getPassword() != null && creds.getPassword().length == getPassword().length) {
					pwdEquals = true;
					for(int i = 0; i < getPassword().length; i++) {
						if(getPassword()[i] != creds.getPassword()[i]) {
							pwdEquals = false;
						}
					}
				}
			} else {
				if(getPassword() == null && creds.getPassword() == null) {
					pwdEquals = true;
				}
			}
			return userEquals && pwdEquals;
		} else {
			return false;
		}
	}
	
	@Override
	public String toString() {
		return "\"" + ((username != null)?username:"<null>") + "\":\"" + ((getPassword() != null)?"*******\"":"<null>\"");
	}
}
