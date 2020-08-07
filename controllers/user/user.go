package user

import "github.com/sethvargo/go-password/password"

type User struct {
	Username string `json:"username"`
	Password string `json:"password"`
	// TODO: implement optional grants
}

func NewUser() (*User, error) {
	user := new(User)
	pass, err := password.Generate(12, 5, 3, false, false)
	if err != nil {
		return user, err
	}
	user.Password = pass
	return user, nil
}
