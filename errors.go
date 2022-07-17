package usago

type Error struct {
	id      int
	message string
}

var _ (error) = (*Error)(nil)

func (err *Error) Error() string {
	return err.message
}

func (err *Error) GetId() int {
  return err.id
}

func NewChannelClosedError () *Error {
  return &Error{
    id: 1000,
    message: "channel is now closed",
  }
}

func NewChannelConnectionFailureError() *Error {
  return &Error{
    id: 1001,
    message: "connection has failed to establish",
  }
}

func NewNoConfirmsError() *Error {
  return &Error{
    id: 1002,
    message: "confirms is not enabled",
  }
}

func NewConnectionMissingError() *Error {
  return &Error{
    id: 1003,
    message: "connection missing",
  }
}

func NewChannelMissingError() *Error {
  return &Error{
    id: 1004,
    message: "channel missing",
  }
}
