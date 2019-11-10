package util

import "testing"

func TestValidatePath(t *testing.T) {
	type args struct {
		path string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"valid", args{"/this is / a valid/path"}, false},
		{"valid_1", args{"/name/with.period."}, false},
		{"valid_2", args{"/test\u0020"}, false},
		{"valid_3", args{"/test\u007e"}, false},
		{"valid_4", args{"/test\uffef"}, false},
		{"invalid", args{}, true},
		{"invalid_1", args{""}, true},
		{"invalid_2", args{"not/valid"}, true},
		{"/ends/with/slash/", args{"/ends/with/slash/"}, true},
		{"/double//slash", args{"/double//slash"}, true},
		{"/single/./period", args{"/single/./period"}, true},
		{"/double/../period", args{"/double/../period"}, true},
		{"illegal_char", args{"/test\u0000"}, true},
		{"illegal_char_1", args{"/test\u0001"}, true},
		{"illegal_char_2", args{"/test\u001F"}, true},
		{"illegal_char_3", args{"/test\u007F"}, true},
		{"illegal_char_4", args{"/test\u009F"}, true},
		{"illegal_char_5", args{string([]rune{'/', 't', 'e', 's', 't', 0xd800})}, true},
		{"illegal_char_6", args{"/test\uf8ff"}, true},
		{"illegal_char_7", args{"/test\ufff0"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := ValidatePath(tt.args.path); (err != nil) != tt.wantErr {
				t.Errorf("ValidatePath() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
