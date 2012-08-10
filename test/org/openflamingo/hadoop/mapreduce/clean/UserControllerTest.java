package org.openflamingo.hadoop.mapreduce.clean;

import org.easymock.EasyMock;
import org.junit.Test;

/**
 * Description.
 *
 * @author Youngdeok Kim
 * @since 1.0
 */
public class UserControllerTest {

	@Test
	public void testLogin() throws Exception {
		UserController controller = new UserController();
		UserService userService = EasyMock.createMock(UserService.class);
		controller.userService = userService;

		EasyMock.expect(userService.getUser("1")).andReturn(null);
		EasyMock.replay(userService);

		controller.login("1", "2");

		EasyMock.verify(userService);
		EasyMock.reset(userService);
	}

	@Test
	public void testLogin2() throws Exception {
		UserController controller = new UserController();
		UserService userService = EasyMock.createMock(UserService.class);
		controller.userService = userService;

		User user = new User();
		user.username = "2";
		user.password = "3";

		EasyMock.expect(userService.getUser("2")).andReturn(user);
		EasyMock.replay(userService);

		controller.login("2", "3");

		EasyMock.verify(userService);
		EasyMock.reset(userService);
	}

}
